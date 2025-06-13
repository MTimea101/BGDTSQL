import re
import json
from .database  import get_metadata_file
from BackEnd.Insert_Get_From_Mongo.index_controller import create_mongodb_index
def parse_create_index(stmt, curr_database):

    # CHeck if databes in Use
    if curr_database is None:
        return {"error" : "No databse selected"}
    
    pattern = r"CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)"
    match = re.search(pattern, stmt, re.IGNORECASE)

    if not match:
        return {"error": f"Invalid CREATE INDEX statement: {stmt}"}
    
    index_name = match.group(1)
    table_name = match.group(2)
    columns_str = match.group(3).strip()

    # Parse columns, handling potential whitespace
    columns = [col.strip() for col in columns_str.split(',')]
    
    # Read database metadata
    db_file = get_metadata_file(curr_database)
    try:
        with open(db_file, 'r') as f:
            db_content = json.load(f)
        
        # Check if table exists
        if table_name not in db_content.get("tables", {}):
            return {"error": f"Table '{table_name}' does not exist"}
        
        table_data = db_content["tables"][table_name]
        
        # Check if columns exist in the table
        table_columns = [col["name"] for col in table_data["columns"]]
        for col in columns:
            if col not in table_columns:
                return {"error": f"Column '{col}' does not exist in table '{table_name}'"}
        
        # Initialize indexes array if it doesn't exist
        if "indexes" not in table_data:
            table_data["indexes"] = []
        
        # Check for duplicate index name
        for existing_index in table_data["indexes"]:
            if existing_index["name"] == index_name:
                return {"error": f"Index '{index_name}' already exists on table '{table_name}'"}
        
        # Create the index entry
        index_entry = {
            "name": index_name,
            "columns": columns
        }
        
        # Add the index to the table metadata
        table_data["indexes"].append(index_entry)
        
        # Save the updated metadata
        with open(db_file, 'w') as f:
            json.dump(db_content, f, indent=4)

        # Determine if this is a unique index
        is_unique = any(col in table_data["constraints"].get("unique_key", []) for col in columns)
    
        # Create the actual MongoDB index
        index_result = create_mongodb_index(curr_database, table_name, index_name, columns, is_unique)
        
        if "error" in index_result:
            return index_result
            
        return {"message": f"Index '{index_name}' created on table '{table_name}'"}
        
    except FileNotFoundError:
        return {"error": f"Database metadata file not found for '{curr_database}'"}
    except json.JSONDecodeError:
        return {"error": f"Invalid database metadata file for '{curr_database}'"}