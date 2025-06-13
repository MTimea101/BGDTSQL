import re
import json
from BackEnd.Create.database import get_metadata_file
from BackEnd.Insert_Get_From_Mongo.mongodb import insert_document

def parse_insert(stmt, curr_database):

    if curr_database is None:
        return {"error": f"No Database in USE"}
    
    pattern = r"INSERT\s+INTO\s+(\w+)\s*(\([^)]*\))?\s+VALUES\s*\((.*)\)"
    match = re.search(pattern, stmt, re.IGNORECASE)
    if not match:
        return {"error": f"Invalid INSERT statement: {stmt}"}
    
    table_name = match.group(1)
    values_str = match.group(3)

    db_file = get_metadata_file(curr_database)
    with open(db_file, 'r') as f:
        db_content = json.load(f)

    if table_name not in db_content.get("tables", {}):
        return {"error": f"Table '{table_name}' does not exist"}
    
    table_data = db_content["tables"][table_name]
    columns = table_data["columns"]

    values = []
    current = ""
    in_quotes = False #So we can differenciate betweeen {'Alma , Szia'} Or {'ad' , 'adsasd'}
    for char in values_str:
        if char == "'" or char == '"':
            in_quotes = not in_quotes
            current += char
        elif char == ',' and not in_quotes:
            values.append(current.strip())
            current = ""
        else:
            current += char
    
    if current:
        values.append(current.strip())
    
    #Clean up the values ("", '')
    cleaned_values = []
    for val in values:
        val = val.strip()
        if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        cleaned_values.append(val)
    
    if len(cleaned_values) != len(columns):
        return {"error": f"Number of values ({len(cleaned_values)}) does not match number of columns ({len(columns)})"}
    
    primary_keys = table_data["constraints"]["primary_key"]
    if not primary_keys:
        return {"error": "Table must have a primary key defined"}
    
    is_valid, error_message = validate_values(columns, cleaned_values)
    if not is_valid:
        return {"error": error_message}
    
    key_parts = []
    for pk in primary_keys:
        pk_index = next((i for i, col in enumerate(columns) if col["name"] == pk), None)
        if pk_index is None:
            return {"error": f"Primary key column '{pk}' not found"}
        key_parts.append(cleaned_values[pk_index])
      
    document_key = "$".join(key_parts) if len(key_parts) > 1 else key_parts[0]

    value_parts = []
    for i, col in enumerate(columns):
        if col["name"] not in primary_keys:
            value_parts.append(cleaned_values[i])
    
    document_value = "#".join(value_parts)
    
    # Insert into MongoDB
    return insert_document(curr_database, table_name, document_key, document_value,columns, cleaned_values)

def validate_values(columns, values):

    for i, (col, val) in enumerate(zip(columns, values)):
        col_type = col["type"].upper()

        # Validate INT type
        if col_type == "INT":
            try:
                int(val)
            except ValueError:
                return False, f"Value '{val}' is not a valid INT for column '{col['name']}'"
        
        # Validate FLOAT type
        elif col_type == "FLOAT":
            try:
                float(val)
            except ValueError:
                return False, f"Value '{val}' is not a valid FLOAT for column '{col['name']}'"
        
        # Validate BOOL type
        elif col_type == "BOOL":
            if val.upper() not in ["TRUE", "FALSE", "1", "0"]:
                return False, f"Value '{val}' is not a valid BOOL for column '{col['name']}'"
        
        # Validate DATE type
        elif col_type == "DATE":
            # Simple date validation
            date_pattern = r"^\d{4}[.-]\d{2}[.-]\d{2}$"
            if not re.match(date_pattern, val):
                return False, f"Value '{val}' is not a valid DATE for column '{col['name']}'"
        
        # Validate VARCHAR type
        elif col_type.startswith("VARCHAR"):
            # Extract the size limit from VARCHAR(size)
            size_match = re.match(r"VARCHAR\((\d+)\)", col_type)
            if size_match:
                max_size = int(size_match.group(1))
                if len(val) > max_size:
                    return False, f"Value '{val}' exceeds maximum length {max_size} for column '{col['name']}'"
    
    # All values passed validation
    return True, ""
