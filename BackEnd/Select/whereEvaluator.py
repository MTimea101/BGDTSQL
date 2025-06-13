# whereEvaluator.py - Javított verzió
def apply_where_conditions(doc, metadata, conditions, needed_columns=None):
    """Javított WHERE feltétel alkalmazás"""
    
    if needed_columns is None:
        needed_columns = {cond["column"] for cond in conditions}
    
    if not conditions:
        return True
    
    # Az _id mező feldolgozása (elsődleges kulcs)
    pk_cols = metadata["constraints"].get("primary_key", [])
    pk_parts = doc["_id"].split("$") if "$" in doc["_id"] else [doc["_id"]]
    
    column_map = {}
    
    # JAVÍTÁS: MINDEN elsődleges kulcs oszlop feldolgozása (nem csak a szükségesek)
    for i, col in enumerate(pk_cols):
        column_map[col] = pk_parts[i] if i < len(pk_parts) else ""
    
    # JAVÍTÁS: MINDEN nem-elsődleges kulcs oszlop feldolgozása
    if "value" in doc and doc["value"]:
        values = doc["value"].split("#")
        non_pk_cols = [col["name"] for col in metadata["columns"] if col["name"] not in pk_cols]
        for i, col in enumerate(non_pk_cols):
            if i < len(values):
                column_map[col] = values[i]

    # Feltételek ellenőrzése
    for cond in conditions:
        col, op, val = cond["column"], cond["op"], cond["value"]
        doc_val = column_map.get(col)
        
        # JAVÍTÁS: Ha az oszlop nem található, false visszatérés
        if doc_val is None:
            print(f"DEBUG: Column '{col}' not found in document. Available columns: {list(column_map.keys())}")
            return False

        # JAVÍTÁS: Jobb típuskonverzió és összehasonlítás
        if not compare_values(doc_val, op, val):
            return False

    return True

def compare_values(doc_val, op, target_val):
    """Javított értékösszehasonlítás"""
    try:
        # Próbáljuk meg numerikus összehasonlításként
        doc_val_num = float(doc_val)
        target_val_num = float(target_val)
        
        if op == "=":
            return doc_val_num == target_val_num
        elif op == ">":
            return doc_val_num > target_val_num
        elif op == "<":
            return doc_val_num < target_val_num
        elif op == ">=":
            return doc_val_num >= target_val_num
        elif op == "<=":
            return doc_val_num <= target_val_num
        else:
            return False
            
    except (ValueError, TypeError):
        # Ha nem numerikus, akkor szöveges összehasonlítás
        doc_val_str = str(doc_val).strip()
        target_val_str = str(target_val).strip()
        
        if op == "=":
            return doc_val_str == target_val_str
        elif op in [">", "<", ">=", "<="]:
            # Szöveges összehasonlítás lexikografikusan
            if op == ">":
                return doc_val_str > target_val_str
            elif op == "<":
                return doc_val_str < target_val_str
            elif op == ">=":
                return doc_val_str >= target_val_str
            elif op == "<=":
                return doc_val_str <= target_val_str
        
        return False
