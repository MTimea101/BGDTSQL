import re
from .database import *

def check_table_name(table_name, curr_database):
    curr = get_metadata_file(curr_database)
    print(curr)
    with open(curr, "r", encoding="utf-8") as file:
        data = json.load(file)
    return table_name in data.get("tables", {})

def parse_create_table(sql, curr_database):
    if not isinstance(sql, str):
        raise ValueError("SQL must be a string")
    
    # Match CREATE TABLE statement
    table_pattern = r"CREATE TABLE (\w+)\s*\(((?:[^()]|\([^()]*\))*)\)"
    match = re.search(table_pattern, sql, re.IGNORECASE | re.DOTALL)
    print(sql)
    if not match:
        return {"error": "Invalid SQL syntax"}
    
    table_name = match.group(1)
    if check_table_name(table_name, curr_database):
        return {"error" : f"Table '{table_name}' already exists"}
    columns_definition = match.group(2).strip()

    # Column regex: Allows additional constraints
    column_pattern = r"(\w+)\s+([\w()]+)(?:\s+(PRIMARY\s+KEY|UNIQUE))?$"
    
    # Foreign Key regex (inside CREATE TABLE)
    foreign_key_pattern = r"(\w+)\s+([\w()]+)\s+REFERENCES\s+(\w+)\s*\((\w+)\)"
    pk_constraint_pattern = r"PRIMARY\s+KEY\s*\(([^)]+)\)"

    valid_types = {"INT", "FLOAT", "BOOL", "TEXT", "DATE"}
    columns = []
    unique_constraints = []
    primary_keys = []
    foreign_keys = []
    
    column_lines = []
    current_column = ""
    paren_level = 0
    for char in columns_definition:
        if char == '(':
            paren_level += 1
            current_column += char
        elif char == ')':
            paren_level -= 1
            current_column += char
        elif char == ',' and paren_level == 0:
            # Only split at commas when not inside parentheses
            if current_column.strip():
                column_lines.append(current_column.strip())
            current_column = ""
        else:
            current_column += char
    
    # Don't forget the last column definition
    if current_column.strip():
        column_lines.append(current_column.strip())

    for col_line in column_lines:
       
        print(col_line)
        pk_match = re.match(pk_constraint_pattern, col_line, re.IGNORECASE)
        if pk_match:
            # Extract columns from the PRIMARY KEY constraint
            pk_columns = [col.strip() for col in pk_match.group(1).split(',')]
            primary_keys.extend(pk_columns)
            continue

        col_match = re.match(column_pattern, col_line, re.IGNORECASE)
        if col_match:
            col_name, col_type, constraint_flag = col_match.groups()
            
            col_type = col_type.upper()
            if not (col_type in valid_types or re.match(r"VARCHAR\(\d+\)", col_type)):
                return {"error": f"Invalid column type '{col_type}' for column '{col_name}'"}

            columns.append({"name": col_name, "type": col_type})

            #Keys 
            if constraint_flag:
                constraint_flag = constraint_flag.upper()
                if constraint_flag == "PRIMARY KEY":
                    primary_keys.append(col_name)
                elif constraint_flag == "UNIQUE":
                    unique_constraints.append(col_name)
        else:
            # Check if it's a foreign key definition
            fk_match = re.match(foreign_key_pattern, col_line, re.IGNORECASE)
            if fk_match:
                col_name, col_type, ref_table, foreign_key_flag = fk_match.groups()
    
                col_type = col_type.upper()
                if not (col_type in valid_types or re.match(r"VARCHAR\(\d+\)", col_type)):
                    return {"error": f"Invalid column type '{col_type}' for column '{col_name}'"}

                db_file = get_metadata_file(curr_database)
                with open(db_file, 'r') as f:
                    db_content = json.load(f)
                
                # Check if referenced table exists
                if ref_table not in db_content.get("tables", {}):
                    return {"error": f"Referenced table '{ref_table}' does not exist"}
                
                # Check if referenced column exists in the referenced table
                ref_table_data = db_content["tables"][ref_table]
                ref_columns = [col["name"] for col in ref_table_data["columns"]]
                
                if foreign_key_flag not in ref_columns:
                    return {"error": f"Referenced column '{foreign_key_flag}' does not exist in table '{ref_table}'"}
                
                # Check if referenced column is a primary key
                if foreign_key_flag not in ref_table_data["constraints"]["primary_key"]:
                    return {"error": f"Referenced column '{foreign_key_flag}' must be a primary key in table '{ref_table}'"}
                
                columns.append({"name": col_name, "type": col_type})
                foreign_keys.append({
                    "column": col_name,
                    "references": {
                        "table":ref_table,
                        "column": foreign_key_flag
                    }
                })
            else:
                return {"error": f"Invalid column definition: '{col_line}'"}

    column_names = [col["name"] for col in columns]
    for pk in primary_keys:
        if pk not in column_names:
            return {"error": f"Primary key column '{pk}' does not exist in table definition"}
    
    # Ensure at least one primary key is defined
    if not primary_keys:
        return {"error": "Table must have at least one primary key column defined"}

    result = {
        "table_name": table_name,
        "columns": columns,
        "constraints": {
            "primary_key": primary_keys,
            "unique_key":unique_constraints,
            "foreign_keys": foreign_keys,
        }
    }

    return result
