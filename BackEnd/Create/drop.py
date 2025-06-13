import re
from .database import *

def parse_drop_table(stmt, curr_database):
    curr = get_metadata_file(curr_database)
    
    match = re.search(r'DROP TABLE (\w+)', stmt, re.IGNORECASE)

    if match:
        table_name = match.group(1)
    else:
        return {"error": f"Invalid DROP statment: {stmt}"}
    
    with open(curr, "r", encoding="utf-8") as file:
        data = json.load(file)
    
    if table_name in data.get("tables", {}):
        del data["tables"][table_name]  # Remove table entry
    else:
        return {"error": f"Table does not exist {table_name}"}
    
    # Save changes back to file
    with open(curr, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    
    return {"message": f"Table '{table_name}' has been dropped successfully"}

def parse_drop_database(stmt, curr_database):
    match = re.search(r'DROP DATABASE (\w+)', stmt, re.IGNORECASE)
    
    if match:
        database_name = match.group(1)
    else:
        return {"error": f"Invalid DROP statement: {stmt}"}
    
    # Check if the current database matches the one being dropped
    if curr_database == database_name:
        return {"error": "Cannot drop the current database"}
    
    metadata_file = get_metadata_file(database_name)

    if not os.path.exists(metadata_file):
        return {"error": "Database file does not exist"}
    
    # Remove the database file from the filesystem
    os.remove(metadata_file)
    
    return {"message": f"Database '{database_name}' has been dropped successfully"}
