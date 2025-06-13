import re
import json
from BackEnd.Create.database import get_metadata_file
from BackEnd.Insert_Get_From_Mongo.mongodb import delete_document

def parse_delete(stmt, curr_database):
   
    if curr_database is None:
        return {"error": "No database selected"}

    # Parse the table name and where clause
    pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
    match = re.search(pattern, stmt, re.IGNORECASE)
    
    if not match:
        return {"error": f"Invalid DELETE statement: {stmt}"}
    
    table_name = match.group(1)
    where_clause = match.group(2)
    
    # Get table metadata
    db_file = get_metadata_file(curr_database)
    with open(db_file, 'r') as f:
        db_content = json.load(f)
    
    # Check if table exists
    if table_name not in db_content.get("tables", {}):
        return {"error": f"Table '{table_name}' does not exist"}
    
    table_data = db_content["tables"][table_name]
    
    # If no WHERE clause, return an error (don't allow DELETE without WHERE)
    if not where_clause:
        return {"error": "DELETE statements must include a WHERE clause"}
    
    # Parse the WHERE clause
    conditions = {}
    for condition in where_clause.split(" AND "):
        parts = condition.strip().split("=")
        if len(parts) != 2:
            return {"error": f"Invalid condition: {condition}"}
        
        col = parts[0].strip()
        val = parts[1].strip()
        
        # Remove quotes if present
        if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        
        conditions[col] = val
    
    # Get primary key columns
    primary_keys = table_data["constraints"]["primary_key"]
    if not primary_keys:
        return {"error": "Table must have a primary key defined"}
    
    # Check if all primary key columns are specified in WHERE
    for pk in primary_keys:
        if pk not in conditions:
            return {"error": f"Primary key column '{pk}' must be specified in WHERE clause"}
    
    # Build the document key
    key_parts = []
    for pk in primary_keys:
        key_parts.append(conditions[pk])
    
    document_key = "$".join(key_parts) if len(key_parts) > 1 else key_parts[0]
    
    # Delete from MongoDB
    return delete_document(curr_database, table_name, document_key)