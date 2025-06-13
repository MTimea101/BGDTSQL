from BackEnd.Insert_Get_From_Mongo.db_connection import get_db_collection
from BackEnd.Select.indexReader import load_index
from BackEnd.Select.whereEvaluator import apply_where_conditions

# Batch size konfigurálása
BATCH_SIZE = 10000  # Egyszerre max 1000 sor memóriában
MAX_JOIN_CACHE_SIZE = 5000  # JOIN táblák cache mérete

def execute_join(database, main_table, joins, selected_columns, conditions, metadata_all, is_distinct=False):
    all_tables = [main_table] + [join["table"] for join in joins]
    
    # 1. WHERE feltételek szétválasztása MINDEN TÁBLÁRA külön
    table_conditions = separate_conditions_by_table(conditions, all_tables, metadata_all)
    
    # 2. Index cache betöltése (csak metadata, nem a teljes adat)
    index_cache = preload_all_indexes(database, joins, all_tables, metadata_all)
    
    # 3. JOIN táblák query-inek előkészítése (NEM betöltés!)
    join_queries = prepare_join_table_queries(database, table_conditions, joins, metadata_all)
    
    # 4. Main table batch-enkénti feldolgozása
    result_rows = []
    main_query = build_query_for_table(database, main_table, table_conditions.get(main_table, []), metadata_all[main_table])
    
    batch_count = 0
    for main_batch in get_table_batches(main_query, BATCH_SIZE):
        batch_count += 1
        print(f"Processing main table batch {batch_count} with {len(main_batch)} records")
        
        # Batch-re JOIN végrehajtás
        batch_results = process_main_batch(
            database, main_batch, main_table, joins, 
            join_queries, selected_columns, conditions, 
            metadata_all, index_cache
        )
        
        result_rows.extend(batch_results)
        
        # Memória monitoring
        if len(result_rows) % 10000 == 0:
            print(f"Accumulated results: {len(result_rows)} rows")
    
    # 5. DISTINCT kezelése (csak a végén, hogy ne duplikáljunk batch-enként)
    if is_distinct:
        unique_rows_set = set(tuple(row) for row in result_rows)
        result_rows = [list(row) for row in unique_rows_set]
    
    print(f"JOIN completed: {len(result_rows)} final records from {batch_count} batches")
    actual_headers = []
    for col in selected_columns:
        if col == "*":
            # * esetén minden tábla minden oszlopát hozzáadjuk
            for table in all_tables:
                table_metadata = metadata_all[table]
                for table_col in table_metadata["columns"]:
                    actual_headers.append(f"{table}.{table_col['name']}")
        else:
            actual_headers.append(col)
            
    return {
        "headers": actual_headers,
        "rows": result_rows
    }

def get_table_batches(query_info, batch_size):
    """Batch-enkénti lekérés MongoDB cursor-ral"""
    collection = query_info["collection"]
    query_filter = query_info["filter"]
    projection = query_info["projection"]
    
    cursor = collection.find(query_filter, projection).batch_size(batch_size)
    
    batch = []
    for doc in cursor:
        batch.append(doc)
        
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Utolsó (nem teljes) batch
    if batch:
        yield batch

def build_query_for_table(database, table_name, conditions, table_metadata):
    """JAVÍTOTT: Main table-ből is megfelelően betöltjük a value mezőt"""
    collection = get_db_collection(database, table_name)
    
    # Index használható feltételek feldolgozása
    query_filter = {}
    remaining_conditions = []
    
    # Megkeressük az indexed oszlopokat
    indexed_columns = set()
    for idx in table_metadata.get("indexes", []):
        if isinstance(idx, dict) and "columns" in idx:
            indexed_columns.update(idx["columns"])
    
    # MongoDB query építése index feltételekből
    for cond in conditions:
        column = cond["column"]
        
        if column in indexed_columns:
            if cond["op"] == "=":
                pass  # Ezt külön kezeljük load_indexed_ids_for_conditions-ben
            else:
                remaining_conditions.append(cond)
        else:
            remaining_conditions.append(cond)
    
    # Index-alapú ID-k lekérése
    indexed_ids = load_indexed_ids_for_conditions(database, table_name, conditions, table_metadata)
    
    if indexed_ids is not None:
        query_filter["_id"] = {"$in": list(indexed_ids)}
    
    # JAVÍTOTT PROJEKCIÓ LOGIKA - MINDIG BETÖLTJÜK A VALUE MEZŐT
    projection = {"_id": 1, "value": 1}  # Mindig betöltjük mindkét mezőt
    
    return {
        "collection": collection,
        "filter": query_filter,
        "projection": projection,
        "remaining_conditions": remaining_conditions,
        "table_metadata": table_metadata,
        "table_name": table_name
    }

def load_indexed_ids_for_conditions(database, table_name, conditions, table_metadata):
    """Index-ek használatával ID-k lekérése WHERE feltételekhez"""
    matching_ids_sets = []
    
    indexed_columns = set()
    for idx in table_metadata.get("indexes", []):
        if isinstance(idx, dict) and "columns" in idx:
            indexed_columns.update(idx["columns"])
    
    for cond in conditions:
        if cond["column"] in indexed_columns and cond["op"] == "=":
            # Keresünk megfelelő indexet
            best_index = find_best_index_for_column(table_metadata, cond["column"])
            if best_index:
                index_data = load_index(database, table_name, best_index["name"])
                if index_data and cond["value"] in index_data:
                    ids = index_data[cond["value"]]
                    if isinstance(ids, str):
                        ids = [ids]
                    matching_ids_sets.append(set(ids))
    
    if matching_ids_sets:
        # Metszet az összes index feltételből
        return set.intersection(*matching_ids_sets)
    
    return None

def prepare_join_table_queries(database, table_conditions, joins, metadata_all):
    """JOIN táblák query-inek előkészítése (betöltés nélkül)"""
    join_queries = {}
    
    for join in joins:
        table = join["table"]
        table_conds = table_conditions.get(table, [])
        
        # Eredeti query info lekérése
        query_info = build_query_for_table(
            database, table, table_conds, metadata_all[table]
        )
        
        # JAVÍTÁS: A projekció már megfelelően van beállítva a build_query_for_table-ben
        # Nincs szükség további módosításra
        
        join_queries[table] = query_info
    
    return join_queries

def process_main_batch(database, main_batch, main_table, joins, join_queries, selected_columns, conditions, metadata_all, index_cache):
    """Javított batch feldolgozás"""
    batch_results = []
    join_cache = {}
    
    for main_doc in main_batch:
        # JAVÍTÁS: Main table WHERE feltételek alkalmazása
        main_table_conditions = []
        for cond in conditions:
            column = cond["column"]
            # Ha prefix nélküli vagy main table prefixű
            if "." not in column:
                # Ellenőrizzük, hogy a main table-ben van-e ilyen oszlop
                main_table_columns = [col["name"] for col in metadata_all[main_table]["columns"]]
                if column in main_table_columns:
                    main_table_conditions.append(cond)
            elif column.startswith(f"{main_table}."):
                cond_copy = cond.copy()
                cond_copy["column"] = column.split(".")[1]
                main_table_conditions.append(cond_copy)
        
        # Main sor szűrése
        if main_table_conditions and not apply_where_conditions(main_doc, metadata_all[main_table], main_table_conditions):
            continue
        
        # JOIN végrehajtás
        joined_row = build_row_from_doc(main_doc, main_table, metadata_all[main_table])
        
        join_results = execute_joins_with_batch_cache(
            database, joined_row, joins, 0, join_queries, 
            join_cache, index_cache, metadata_all
        )
        
        for join_result in join_results:
            # JAVÍTÁS: Cross-table WHERE feltételek
            cross_table_conditions = []
            for cond in conditions:
                column = cond["column"]
                if "." in column:
                    table_name = column.split(".")[0]
                    if table_name != main_table and table_name in [j["table"] for j in joins]:
                        cross_table_conditions.append(cond)
                elif column not in [col["name"] for col in metadata_all[main_table]["columns"]]:
                    # Nem main table oszlop - lehet JOIN table oszlop
                    cross_table_conditions.append(cond)
            
            if apply_cross_table_conditions(join_result, cross_table_conditions, [main_table] + [j["table"] for j in joins], metadata_all):
                selected_row = select_join_columns(join_result, selected_columns, [main_table] + [j["table"] for j in joins], metadata_all)
                batch_results.append(selected_row)
    
    return batch_results

def execute_joins_with_batch_cache(database, current_row, joins, join_index, join_queries, join_cache, index_cache, metadata_all):
    """JOIN végrehajtás batch-specifikus cache-sel"""
    if join_index >= len(joins):
        return [current_row]
    
    current_join = joins[join_index]
    join_table = current_join["table"]
    left_column = current_join["left_column"]
    right_column = current_join["right_column"].split(".")[-1]
    
    left_value = get_column_value_from_row(current_row, left_column)
    if left_value is None:
        return []
    
    # Cache kulcs a JOIN értékhez
    cache_key = f"{join_table}_{right_column}_{left_value}"
    
    # Cache ellenőrzés
    if cache_key in join_cache:
        matching_docs = join_cache[cache_key]
    else:
        # Cache-ben nincs: lekérés és cache-lés
        matching_docs = find_matching_docs_with_batch_optimization(
            database, join_table, right_column, left_value,
            join_queries[join_table], index_cache, metadata_all[join_table]
        )
        
        # Cache size limit ellenőrzés
        if len(join_cache) < MAX_JOIN_CACHE_SIZE:
            join_cache[cache_key] = matching_docs
        else:
            # Cache full: LRU vagy nincs cache
            pass
    
    matching_rows = []
    for join_doc in matching_docs:
        join_row = build_row_from_doc(join_doc, join_table, metadata_all[join_table])
        combined_row = {**current_row, **join_row}
        
        # Rekurzív következő JOIN
        sub_results = execute_joins_with_batch_cache(
            database, combined_row, joins, join_index + 1,
            join_queries, join_cache, index_cache, metadata_all
        )
        matching_rows.extend(sub_results)
    
    return matching_rows

def find_matching_docs_with_batch_optimization(database, table, column, value, query_info, index_cache, table_metadata):
    """JOIN oszlop alapján keresés batch-optimalizált módon"""
    
    # 1. Index használat prioritása
    matching_docs = []
    
    # Primary Key ellenőrzés
    primary_keys = table_metadata.get("constraints", {}).get("primary_key", [])
    if column in primary_keys:
        matching_docs = search_by_primary_key_with_query(
            database, table, column, value, primary_keys, query_info
        )
        return matching_docs
    
    # 2. Explicit index használat
    for index_key, index_info in index_cache.items():
        if (index_key.startswith(f"{table}_") and 
            index_info.get("type") in ["explicit", "where_index"] and
            column in index_info.get("metadata", {}).get("columns", [])):
            
            matching_docs = search_with_index_and_query(
                database, table, column, value, index_info, query_info
            )
            return matching_docs
    
    # 3. Fallback: filtered table scan
    return search_with_table_scan_filtered(database, table, column, value, query_info)

def search_by_primary_key_with_query(database, table, column, value, primary_keys, query_info):
    """Primary key keresés a pre-built query-vel kombinálva"""
    collection = query_info["collection"]
    base_filter = query_info["filter"].copy()
    projection = query_info["projection"]
    
    if len(primary_keys) == 1:
        # Egyszerű PK
        if column == primary_keys[0]:
            base_filter["_id"] = str(value)
    else:
        # Composite PK: regex keresés
        column_position = primary_keys.index(column)
        if column_position == 0:
            base_filter["_id"] = {"$regex": f"^{value}($|\\$)"}
        else:
            # Composite PK közepén: table scan
            return search_with_table_scan_filtered(database, table, column, value, query_info)
    
    return list(collection.find(base_filter, projection))

def search_with_index_and_query(database, table, column, value, index_info, query_info):
    """Index + query kombináció"""
    index_data = index_info["data"]
    index_metadata = index_info["metadata"]
    columns = index_metadata["columns"]
    
    # Index-ből ID-k lekérése
    matching_ids = set()
    column_position = columns.index(column) if column in columns else -1
    
    if column_position == 0:
        # Prefix match
        if str(value) in index_data:
            ids = index_data[str(value)]
            if isinstance(ids, str):
                matching_ids.add(ids)
            else:
                matching_ids.update(ids)
    
    if not matching_ids:
        return []
    
    # Query + index ID-k kombinálása
    collection = query_info["collection"]
    base_filter = query_info["filter"].copy()
    
    if "_id" in base_filter:
        # Metszet a már meglévő ID szűrővel
        existing_ids = base_filter["_id"]["$in"] if "$in" in base_filter["_id"] else []
        final_ids = matching_ids.intersection(set(existing_ids)) if existing_ids else matching_ids
    else:
        final_ids = matching_ids
    
    base_filter["_id"] = {"$in": list(final_ids)}
    
    return list(collection.find(base_filter, query_info["projection"]))

def search_with_table_scan_filtered(database, table, column, value, query_info):
    """Filtered table scan (query alapú szűréssel)"""
    collection = query_info["collection"]
    base_filter = query_info["filter"]
    projection = query_info["projection"]
    
    matching_docs = []
    
    # Batch-enkénti feldolgozás a table scan-hez is
    cursor = collection.find(base_filter, projection).batch_size(500)
    
    for doc in cursor:
        doc_row = build_row_from_doc(doc, table, query_info["table_metadata"])
        if str(get_column_value_from_row(doc_row, column)) == str(value):
            matching_docs.append(doc)
            
            # Memory limit a table scan-re
            if len(matching_docs) > 1000:  # Max 1000 match per JOIN
                break
    
    return matching_docs

# Eredeti helper függvények változatlanul (csak a neveket újrahasznosítjuk)
def separate_conditions_by_table(conditions, all_tables, metadata_all):
    """Javított feltétel szétválasztás"""
    table_conditions = {}
    cross_table_conditions = []
    
    for condition in conditions:
        column = condition["column"]
        
        # JAVÍTÁS: Pontosabb tábla azonosítás
        if "." in column:
            # Explicit tábla.oszlop formátum
            table_name = column.split(".")[0]
            col_name = column.split(".")[1]
            
            if table_name in all_tables:
                # Ellenőrizzük, hogy az oszlop létezik-e a táblában
                table_columns = [col["name"] for col in metadata_all[table_name]["columns"]]
                if col_name in table_columns:
                    if table_name not in table_conditions:
                        table_conditions[table_name] = []
                    condition_copy = condition.copy()
                    condition_copy["column"] = col_name  # Prefix nélkül
                    table_conditions[table_name].append(condition_copy)
                else:
                    print(f"WARNING: Column '{col_name}' not found in table '{table_name}'")
                    cross_table_conditions.append(condition)
            else:
                cross_table_conditions.append(condition)
        else:
            # Oszlop név prefix nélkül - keresés minden táblában
            found_tables = []
            for table in all_tables:
                table_columns = [col["name"] for col in metadata_all[table]["columns"]]
                if column in table_columns:
                    found_tables.append(table)
            
            if len(found_tables) == 1:
                # Egyértelmű: csak egy táblában van ilyen oszlop
                table = found_tables[0]
                if table not in table_conditions:
                    table_conditions[table] = []
                table_conditions[table].append(condition)
            elif len(found_tables) > 1:
                # Többértelmű: több táblában is van - cross-table condition
                print(f"WARNING: Column '{column}' found in multiple tables: {found_tables}")
                cross_table_conditions.append(condition)
            else:
                # Nem található egyik táblában sem
                print(f"WARNING: Column '{column}' not found in any table")
                cross_table_conditions.append(condition)
    
    if cross_table_conditions:
        table_conditions["cross_table"] = cross_table_conditions
    
    print(f"DEBUG: Table conditions: {table_conditions}")
    return table_conditions

def preload_all_indexes(database, joins, all_tables, metadata_all):
    """Index metadata betöltése (teljes adat nélkül ahol lehetséges)"""
    index_cache = {}
    
    # JOIN indexek
    for join in joins:
        table = join["table"]
        right_column = join["right_column"].split(".")[-1]
        
        # Primary Key info
        primary_keys = metadata_all[table].get("constraints", {}).get("primary_key", [])
        if right_column in primary_keys:
            index_key = f"{table}_PRIMARY_KEY"
            index_cache[index_key] = {
                "type": "primary_key",
                "column": right_column,
                "primary_keys": primary_keys
            }
            continue
        
        # Explicit indexek
        suitable_indexes = find_suitable_indexes(metadata_all[table], right_column)
        best_index = select_best_index(suitable_indexes, right_column)
        
        if best_index:
            index_key = f"{table}_{best_index['name']}_JOIN"
            loaded_index = load_index(database, table, best_index["name"])
            if loaded_index:
                index_cache[index_key] = {
                    "type": "explicit",
                    "data": loaded_index,
                    "metadata": best_index
                }
    
    # WHERE indexek
    for table in all_tables:
        table_metadata = metadata_all[table]
        for idx in table_metadata.get("indexes", []):
            if isinstance(idx, dict) and "name" in idx:
                index_key = f"{table}_{idx['name']}_WHERE"
                if index_key not in index_cache:
                    loaded_index = load_index(database, table, idx["name"])
                    if loaded_index:
                        index_cache[index_key] = {
                            "type": "where_index",
                            "data": loaded_index,
                            "metadata": idx,
                            "table": table
                        }
    
    return index_cache

# További helper függvények (változatlanul az eredeti kódból)
def find_best_index_for_column(table_metadata, column):
    suitable_indexes = []
    
    for idx in table_metadata.get("indexes", []):
        if isinstance(idx, dict) and "columns" in idx:
            if column in idx["columns"]:
                position = idx["columns"].index(column)
                suitable_indexes.append({
                    "name": idx["name"],
                    "columns": idx["columns"],
                    "position": position,
                    "column_count": len(idx["columns"])
                })
    
    if not suitable_indexes:
        return None
    
    single_column = [idx for idx in suitable_indexes if idx["column_count"] == 1 and idx["position"] == 0]
    if single_column:
        return single_column[0]
    
    prefix_indexes = [idx for idx in suitable_indexes if idx["position"] == 0]
    if prefix_indexes:
        return min(prefix_indexes, key=lambda x: x["column_count"])
    
    other_indexes = [idx for idx in suitable_indexes if idx["position"] > 0]
    if other_indexes:
        return min(other_indexes, key=lambda x: (x["position"], x["column_count"]))
    
    return None

def apply_conditions_to_row(row, conditions):
    for cond in conditions:
        column = cond["column"]
        op = cond["op"]
        target_value = cond["value"]
        
        actual_value = row.get(column)
        if actual_value is None:
            return False
        
        if not compare_values(actual_value, op, target_value):
            return False
    
    return True

def find_suitable_indexes(table_metadata, target_column):
    suitable_indexes = []
    
    for idx in table_metadata.get("indexes", []):
        if isinstance(idx, dict) and "columns" in idx:
            columns = idx["columns"]
            
            if target_column in columns:
                position = columns.index(target_column)
                suitable_indexes.append({
                    "name": idx["name"],
                    "columns": columns,
                    "position": position,
                    "column_count": len(columns)
                })
    
    return suitable_indexes

def select_best_index(suitable_indexes, target_column):
    if not suitable_indexes:
        return None
    
    single_column_indexes = [idx for idx in suitable_indexes if idx["column_count"] == 1 and idx["position"] == 0]
    if single_column_indexes:
        return single_column_indexes[0]
    
    prefix_indexes = [idx for idx in suitable_indexes if idx["position"] == 0]
    if prefix_indexes:
        return min(prefix_indexes, key=lambda x: x["column_count"])
    
    other_indexes = [idx for idx in suitable_indexes if idx["position"] > 0]
    if other_indexes:
        return min(other_indexes, key=lambda x: (x["position"], x["column_count"]))
    
    return None

def build_row_from_doc(doc, table_name, table_metadata):
    """JAVÍTOTT: Megfelelően feldolgozza a dokumentumot row-vá"""
    row = {}
    pk_cols = table_metadata["constraints"].get("primary_key", [])
    pk_parts = doc["_id"].split("$") if "$" in doc["_id"] else [doc["_id"]]
    
    # Primary key oszlopok feldolgozása
    for i, col in enumerate(pk_cols):
        prefixed_col = f"{table_name}.{col}"
        value = pk_parts[i] if i < len(pk_parts) else ""
        row[prefixed_col] = value
        row[col] = value
    
    # Non-primary key oszlopok feldolgozása
    if "value" in doc and doc["value"]:
        values = doc["value"].split("#")
        non_pk_cols = [col["name"] for col in table_metadata["columns"] if col["name"] not in pk_cols]
        
        for i, col in enumerate(non_pk_cols):
            if i < len(values):
                prefixed_col = f"{table_name}.{col}"
                row[prefixed_col] = values[i]
                row[col] = values[i]
    
    return row

def get_column_value_from_row(row, column_name):
    if column_name in row:
        return row[column_name]
    
    if "." not in column_name:
        for key in row:
            if key.endswith(f".{column_name}"):
                return row[key]
    
    return None

def apply_cross_table_conditions(row, conditions, all_tables, metadata_all):
    if not conditions:
        return True
    
    for cond in conditions:
        column = cond["column"]
        op = cond["op"]
        target_value = cond["value"]
        
        actual_value = get_column_value_from_row(row, column)
        if actual_value is None or not compare_values(actual_value, op, target_value):
            return False
    
    return True

def select_join_columns(row, selected_columns, tables, metadata_all):
    result = []
    for col in selected_columns:
        if col == "*":
            for table in tables:
                table_metadata = metadata_all[table]
                for table_col in table_metadata["columns"]:
                    col_name = table_col["name"]
                    value = get_column_value_from_row(row, f"{table}.{col_name}")
                    result.append(value if value is not None else "")
        else:
            value = get_column_value_from_row(row, col)
            result.append(value if value is not None else "")
    return result

def compare_values(actual, operator, target):
    try:
        actual_num = float(actual)
        target_num = float(target)
        
        if operator == "=":
            return actual_num == target_num
        elif operator == ">":
            return actual_num > target_num
        elif operator == "<":
            return actual_num < target_num
        elif operator == ">=":
            return actual_num >= target_num
        elif operator == "<=":
            return actual_num <= target_num
    except ValueError:
        if operator == "=":
            return str(actual) == str(target)
        else:
            return False
    
    return False