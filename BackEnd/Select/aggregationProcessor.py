from collections import defaultdict
import statistics

def process_group_by_and_aggregations(rows, headers, group_by_columns, aggregations, order_by_columns=None):
    """
    GROUP BY, aggregációs függvények és ORDER BY feldolgozása az eredménysorokon
    """
    
    if not rows:
        return {"headers": [], "rows": []}
    
    # 1. eset: nincs GROUP BY, de van aggregáció (globális aggregáció)
    if aggregations and not group_by_columns:
        result = process_global_aggregations(rows, headers, aggregations)
    
    # 2. eset: van GROUP BY (lehet aggregációval vagy anélkül)
    elif group_by_columns:
        result = process_group_by_with_aggregations(rows, headers, group_by_columns, aggregations)
    
    # 3. eset: nincs GROUP BY és nincs aggregáció
    else:
        result = {"headers": headers, "rows": rows}
    
    # ORDER BY alkalmazása a végén
    if order_by_columns:
        result = apply_order_by(result["rows"], result["headers"], order_by_columns)
    
    return result

def process_global_aggregations(rows, headers, aggregations):
    """
    Aggregációk kezelése GROUP BY nélkül (az egész eredményhalmaz egy csoport)
    """
    result_row = []
    result_headers = []
    
    for agg in aggregations:
        func = agg["function"]
        column = agg["column"]
        
        # Eredmény fejléc építése
        if func == "COUNT" and column == "*":
            result_headers.append("COUNT(*)")
        else:
            result_headers.append(f"{func}({column})")
        
        # Aggregáció kiszámítása
        if func == "COUNT":
            if column == "*":
                result_row.append(len(rows))
            else:
                # Nem null értékek számolása
                col_index = get_column_index(headers, column)
                if col_index != -1:
                    non_null_count = sum(1 for row in rows if row[col_index] not in [None, "", "NULL"])
                    result_row.append(non_null_count)
                else:
                    result_row.append(0)
        
        elif func in ["SUM", "AVG", "MIN", "MAX"]:
            col_index = get_column_index(headers, column)
            if col_index != -1:
                values = []
                for row in rows:
                    val = row[col_index]
                    if val not in [None, "", "NULL"]:
                        try:
                            values.append(float(val))
                        except (ValueError, TypeError):
                            # Nem numerikus értékek kihagyása
                            pass
                
                if values:
                    if func == "SUM":
                        result_row.append(sum(values))
                    elif func == "AVG":
                        result_row.append(sum(values) / len(values))
                    elif func == "MIN":
                        result_row.append(min(values))
                    elif func == "MAX":
                        result_row.append(max(values))
                else:
                    result_row.append(None)
            else:
                result_row.append(None)
    
    return {"headers": result_headers, "rows": [result_row]}

def process_group_by_with_aggregations(rows, headers, group_by_columns, aggregations):
    """
    GROUP BY kezelése opcionális aggregációkkal
    """
    
    # GROUP BY oszlopok indexeinek lekérése
    group_by_indexes = []
    for col in group_by_columns:
        idx = get_column_index(headers, col)
        if idx == -1:
            raise ValueError(f"A(z) '{col}' oszlop nem található a fejlécek között")
        group_by_indexes.append(idx)
    
    # Sorok csoportosítása GROUP BY oszlopok szerint
    groups = defaultdict(list)
    for row in rows:
        # Kulcs létrehozása GROUP BY értékekből
        group_key = tuple(row[idx] for idx in group_by_indexes)
        groups[group_key].append(row)
    
    # Eredmény fejlécek építése
    result_headers = group_by_columns.copy()
    for agg in aggregations:
        func = agg["function"]
        column = agg["column"]
        
        if func == "COUNT" and column == "*":
            result_headers.append("COUNT(*)")
        else:
            result_headers.append(f"{func}({column})")
    
    # Minden csoport feldolgozása
    result_rows = []
    for group_key, group_rows in groups.items():
        result_row = list(group_key)  # A csoport kulcsával kezdjük
        
        # Aggregációk kiszámítása a csoportban
        for agg in aggregations:
            func = agg["function"]
            column = agg["column"]
            
            if func == "COUNT":
                if column == "*":
                    result_row.append(len(group_rows))
                else:
                    col_index = get_column_index(headers, column)
                    if col_index != -1:
                        non_null_count = sum(1 for row in group_rows if row[col_index] not in [None, "", "NULL"])
                        result_row.append(non_null_count)
                    else:
                        result_row.append(0)
            
            elif func in ["SUM", "AVG", "MIN", "MAX"]:
                col_index = get_column_index(headers, column)
                if col_index != -1:
                    values = []
                    for row in group_rows:
                        val = row[col_index]
                        if val not in [None, "", "NULL"]:
                            try:
                                values.append(float(val))
                            except (ValueError, TypeError):
                                pass
                    
                    if values:
                        if func == "SUM":
                            result_row.append(sum(values))
                        elif func == "AVG":
                            result_row.append(sum(values) / len(values))
                        elif func == "MIN":
                            result_row.append(min(values))
                        elif func == "MAX":
                            result_row.append(max(values))
                    else:
                        result_row.append(None)
                else:
                    result_row.append(None)
        
        result_rows.append(result_row)
    
    return {"headers": result_headers, "rows": result_rows}

def apply_order_by(rows, headers, order_by_columns):
    """
    ORDER BY alkalmazása a sorokra
    """
    if not rows or not order_by_columns:
        return {"headers": headers, "rows": rows}
    
    # ORDER BY oszlopindexek lekérése
    order_by_info = []
    for order_col in order_by_columns:
        col_name = order_col["column"]
        direction = order_col.get("direction", "ASC").upper()
        
        col_index = get_column_index(headers, col_name)
        if col_index == -1:
            raise ValueError(f"A(z) '{col_name}' oszlop nem található a fejlécek között")
        
        order_by_info.append({
            "index": col_index,
            "direction": direction
        })
    
    def sort_key(row):
        key_values = []
        for order_info in order_by_info:
            val = row[order_info["index"]]
            
            # NULL vagy üres értékek kezelése (a végére tesszük)
            if val in [None, "", "NULL"]:
                if order_info["direction"] == "DESC":
                    sort_val = float('-inf')
                else:
                    sort_val = float('inf')
            else:
                try:
                    sort_val = float(val)
                except (ValueError, TypeError):
                    sort_val = str(val)
            
            # DESC esetén fordítva
            if order_info["direction"] == "DESC":
                if isinstance(sort_val, (int, float)):
                    sort_val = -sort_val
                # Szövegeket nem fordítunk itt, azt a reverse kezeli
            
            key_values.append(sort_val)
        
        return key_values
    
    # DESC sorrend esetén reverse szükséges szövegekhez
    has_desc = any(order_info["direction"] == "DESC" for order_info in order_by_info)
    
    try:
        sorted_rows = sorted(rows, key=sort_key)
    except TypeError:
        # Ha különböző típusok problémát okoznak, szövegként hasonlítjuk össze
        sorted_rows = sorted(rows, key=lambda row: [str(row[order_info["index"]]) for order_info in order_by_info])
    
    return {"headers": headers, "rows": sorted_rows}

def get_column_index(headers, column_name):
    """
    Oszlop indexének lekérése a fejléc listából
    """
    # Először pontos egyezés
    try:
        return headers.index(column_name)
    except ValueError:
        pass
    
    # Ha nem található, próbáljuk meg a táblanév nélküli változattal
    if "." in column_name:
        simple_name = column_name.split(".")[-1]
        try:
            return headers.index(simple_name)
        except ValueError:
            pass
    
    # Keressünk olyan oszlopot, amely végződik a megadott névvel (pl. tábla.oszlop)
    for i, header in enumerate(headers):
        if header == column_name or header.endswith(f".{column_name}"):
            return i
    
    return -1
