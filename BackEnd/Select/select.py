from BackEnd.Select.selectParser import parse_select_statement
from BackEnd.Select.whereEvaluator import apply_where_conditions
from BackEnd.Select.indexReader import load_index, get_matching_ids_from_index
from BackEnd.Select.joinExecutor import execute_join
from BackEnd.Create.database import get_metadata_file
from BackEnd.Insert_Get_From_Mongo.db_connection import get_db_collection
from BackEnd.Select.aggregationProcessor import process_group_by_and_aggregations

import json

def parse_select(stmt, curr_database):
    if curr_database is None:
        return {"error": "No database selected"}

    parsed = parse_select_statement(stmt)
    if "error" in parsed:
        return parsed

    # Ellenőrizzük, hogy JOIN vagy egyszerű SELECT
    if parsed.get("type") == "join_select":
        return handle_join_select(parsed, curr_database)
    else:
        return handle_simple_select(parsed, curr_database)

def handle_simple_select(parsed, curr_database):
    table_name = parsed['table']
    selected_columns = parsed['columns']
    conditions = parsed['where']
    is_distinct = parsed.get('distinct', False)
    group_by_columns = parsed.get('group_by', [])
    aggregations = parsed.get('aggregations', [])  
    order_by_columns = parsed.get('order_by', [])

    try:
        with open(get_metadata_file(curr_database), "r") as f:
            db_content = json.load(f)
    except Exception as e:
        return {"error": f"Error reading metadata: {e}"}

    if table_name not in db_content.get("tables", {}):
        return {"error": f"Table '{table_name}' does not exist"}

    table_metadata = db_content["tables"][table_name]

    # Column cleaning - eltávolítjuk a tábla prefixeket egyszerű SELECT esetén
    clean_selected_columns = []
    for col in selected_columns:
        if col == "*":
            clean_selected_columns.append("*")
        else:
            clean_col = col.split(".")[-1] if "." in col else col
            clean_selected_columns.append(clean_col)

    # Clean conditions
    clean_conditions = []
    for cond in conditions:
        clean_cond = cond.copy()
        clean_cond["column"] = cond["column"].split(".")[-1] if "." in cond["column"] else cond["column"]
        clean_conditions.append(clean_cond)

    result = execute_select(curr_database, table_name, clean_selected_columns, clean_conditions, table_metadata, is_distinct)
    
    if group_by_columns or aggregations or order_by_columns:
        processed_result = process_group_by_and_aggregations(
            result["rows"], 
            result["headers"], 
            group_by_columns, 
            aggregations, 
            order_by_columns
        )
        return processed_result
    
    return result

def handle_join_select(parsed, curr_database):
    main_table = parsed['table']
    joins = parsed['joins']
    selected_columns = parsed['columns']
    conditions = parsed['where']
    is_distinct = parsed.get('distinct', False)
    group_by_columns = parsed.get('group_by', [])
    aggregations = parsed.get('aggregations', [])  
    order_by_columns = parsed.get('order_by', [])

    try:
        with open(get_metadata_file(curr_database), "r") as f:
            db_content = json.load(f)
    except Exception as e:
        return {"error": f"Error reading metadata: {e}"}

    # Összegyűjtjük az összes érintett táblát
    all_tables = [main_table] + [join["table"] for join in joins]
    
    # Ellenőrizzük, hogy minden tábla létezik
    metadata_all = {}
    for table in all_tables:
        if table not in db_content.get("tables", {}):
            return {"error": f"Table '{table}' does not exist"}
        metadata_all[table] = db_content["tables"][table]

    # Oszlopok validálása JOIN esetén
    if not validate_join_columns(selected_columns, conditions, metadata_all):
        return {"error": "Invalid column reference in JOIN query"}

    # JOIN végrehajtás - JOIN esetén NEM távolítjuk el a prefixeket
    result = execute_join(curr_database, main_table, joins, selected_columns, conditions, metadata_all, is_distinct)
    
    if group_by_columns or aggregations or order_by_columns:
        processed_result = process_group_by_and_aggregations(
            result["rows"], 
            result["headers"], 
            group_by_columns, 
            aggregations, 
            order_by_columns
        )
        return processed_result
    
    return result

def validate_join_columns(selected_columns, conditions, metadata_all):
    # Összegyűjtjük az összes elérhető oszlopot
    available_columns = set()
    
    for table_name, table_metadata in metadata_all.items():
        for col in table_metadata["columns"]:
            col_name = col["name"]
            # Hozzáadjuk prefix-szel és anélkül is
            available_columns.add(col_name)
            available_columns.add(f"{table_name}.{col_name}")
    
    # Ellenőrizzük a selected oszlopokat (kivéve *)
    for col in selected_columns:
        if col != "*" and col not in available_columns:
            return False
    
    # Ellenőrizzük a WHERE feltételek oszlopait
    for cond in conditions:
        if cond["column"] not in available_columns:
            return False
    
    return True

def execute_select(database, table, selected_columns, conditions, metadata, is_distinct=False):
    """Javított SELECT"""
    collection = get_db_collection(database, table)

    # Index használat előkészítése
    column_to_index_map = {}
    for idx in metadata.get("indexes", []):
        if isinstance(idx, dict) and "name" in idx and "columns" in idx:
            for col in idx["columns"]:
                if col not in column_to_index_map:
                    column_to_index_map[col] = []
                column_to_index_map[col].append(idx["name"])
    
    # JAVÍTÁS: Egyszerűbb megközelítés - használjuk az indexeket ahol lehetséges
    matching_ids_sets = []
    index_cache = {}

    # Index alapú feltételek feldolgozása
    for cond in conditions:
        column = cond["column"]
        if column in column_to_index_map and cond["op"] == "=":
            index_names = column_to_index_map[column]
            
            for index_name in index_names:
                if index_name not in index_cache:
                    index_cache[index_name] = load_index(database, table, index_name)
                
                idx_data = index_cache[index_name]
                
                if idx_data:
                    value = cond["value"]
                    matching_ids = set()
                    
                    # Direkt egyezés
                    if value in idx_data:
                        id_list = idx_data[value]
                        if isinstance(id_list, str):
                            matching_ids.add(id_list)
                        elif isinstance(id_list, list):
                            matching_ids.update(id_list)
                        else:
                            # Ha # szeparált string
                            matching_ids.update(str(id_list).split("#"))
                    
                    # Kompozit index prefix keresés
                    for key in idx_data:
                        if key.startswith(f"{value}$"):
                            id_list = idx_data[key]
                            if isinstance(id_list, str):
                                matching_ids.update(id_list.split("#"))
                            else:
                                matching_ids.update(id_list)
                    
                    if matching_ids:
                        matching_ids_sets.append(matching_ids)
                        break  # Elég egy index

    # JAVÍTÁS: Mindig betöltjük a teljes projekciót WHERE feltételekhez
    projection = {"_id": 1, "value": 1}  # Mindig teljes dokumentum

    # Dokumentumok lekérése
    if matching_ids_sets:
        # Ha van index találat, csak azokat a dokumentumokat kérjük le
        final_ids = set.intersection(*matching_ids_sets) if len(matching_ids_sets) > 1 else matching_ids_sets[0]
        docs = list(collection.find({"_id": {"$in": list(final_ids)}}, projection))
    else:
        # Ha nincs index találat, minden dokumentumot lekérünk
        docs = list(collection.find({}, projection))

    # JAVÍTÁS: Minden WHERE feltételt alkalmazunk memóriában
    matching_rows = []
    for doc in docs:
        if apply_where_conditions(doc, metadata, conditions):
            row = extract_columns(doc, metadata, selected_columns)
            matching_rows.append(row)

    # DISTINCT kezelése
    if is_distinct:
        unique_rows_set = set()
        for row in matching_rows:
            unique_rows_set.add(tuple(row))
        unique_rows = [list(row) for row in unique_rows_set]
    else:
        unique_rows = matching_rows
    
    # Headers generálása
    actual_headers = []
    for col in selected_columns:
        if col == "*":
            for table_col in metadata["columns"]:
                actual_headers.append(table_col["name"])
        else:
            actual_headers.append(col)
    
    print(f"DEBUG: Found {len(unique_rows)} rows matching conditions")
    return {
        "headers": actual_headers,
        "rows": unique_rows
    }

def extract_columns(doc, metadata, selected_columns, needed_columns=None):
    """
    Javított oszlop kinyerés * kezeléssel
    """
    # * kibontása tényleges oszlop nevekre
    expanded_columns = []
    for col in selected_columns:
        if col == "*":
            for table_col in metadata["columns"]:
                expanded_columns.append(table_col["name"])
        else:
            expanded_columns.append(col)
    
    # Szükséges oszlopok meghatározása
    if needed_columns is None:
        needed_columns = set(expanded_columns)
    
    # Primary key oszlopok feldolgozása
    pk_cols = metadata["constraints"].get("primary_key", [])
    pk_parts = doc["_id"].split("$") if "$" in doc["_id"] else [doc["_id"]]
    
    column_map = {}
    
    # Primary key értékek
    for i, col in enumerate(pk_cols):
        if col in needed_columns or "*" in selected_columns:
            column_map[col] = pk_parts[i] if i < len(pk_parts) else ""
    
    # Non-primary key értékek
    if "value" in doc and doc["value"]:
        values = doc["value"].split("#")
        non_pk_cols = [col["name"] for col in metadata["columns"] if col["name"] not in pk_cols]
        for i, col in enumerate(non_pk_cols):
            if (col in needed_columns or "*" in selected_columns) and i < len(values):
                column_map[col] = values[i]
    
    # Eredmény összeállítása a kibontott oszlopok alapján
    return [column_map.get(col, "") for col in expanded_columns]