from pymongo import MongoClient, errors
import json
from BackEnd.Create.database import get_metadata_file
from BackEnd.Insert_Get_From_Mongo.index_controller import extract_values_to_dict, update_indexes
from BackEnd.Insert_Get_From_Mongo.db_connection import client, get_db_collection


def validate_unique_key(database, table, column_name, value, table_data, columns, all_values):
    collection = get_db_collection(database, table)
    
    # Meghatározzuk az oszlop pozícióját a nem-pk oszlopok között
    non_pk_columns = [col["name"] for col in columns if col["name"] not in table_data["constraints"]["primary_key"]]
    col_idx = non_pk_columns.index(column_name)
    
    # Ellenőrizzük minden dokumentumban
    cursor = collection.find()
    for doc in cursor:
        values_array = doc["value"].split("#")
        if col_idx < len(values_array) and values_array[col_idx] == value:
            return False, f"Unique constraint violation: value '{value}' already exists for column '{column_name}'"

    return True, ""

def validate_primary_key(database, table, primary_key):

    collection = get_db_collection(database, table)
    existing_doc = collection.find_one({"_id": primary_key})
    return existing_doc is None

def validate_foreign_key(database, table_data, column_name, value):

    for fk in table_data["constraints"].get("foreign_keys", []):
        if fk["column"] == column_name:
            referenced_table = fk["references"]["table"]
            referenced_column = fk["references"]["column"]
            
            # Load referenced table metadata
            with open(get_metadata_file(database), 'r') as f:
                db_content = json.load(f)
            
            if referenced_table not in db_content.get("tables", {}):
                return False, f"Referenced table '{referenced_table}' does not exist"
            
            ref_table_data = db_content["tables"][referenced_table]
            ref_pk = ref_table_data["constraints"]["primary_key"]
            
            # Make sure referenced column is part of the primary key in referenced table
            if referenced_column not in ref_pk:
                return False, f"Referenced column '{referenced_column}' is not a primary key in '{referenced_table}'"
            
            # Check if value exists in referenced table
            ref_collection = get_db_collection(database, referenced_table)
            # In case of composite primary key, need to find where one part matches
            if len(ref_pk) > 1:
                exists = ref_collection.find_one({"_id": {"$regex": f".*{value}.*"}})
            else:
                exists = ref_collection.find_one({"_id": value})
            if not exists:
                return False, f"Foreign key constraint failed: value '{value}' not found in '{referenced_table}.{referenced_column}'"
    
    return True, ""

def insert_document(database, table, key, values, columns=None, all_values=None):
    try:
        # Load table metadata
        with open(get_metadata_file(database), 'r') as f:
            db_content = json.load(f)
        
        if table not in db_content.get("tables", {}):
            return {"error": f"Table '{table}' does not exist"}
        
        table_data = db_content["tables"][table]
        
        # Get columns if not provided
        if columns is None:
            columns = table_data["columns"]
        
        # Validate primary key uniqueness
        if not validate_primary_key(database, table, key):
            return {"error": f"Primary key '{key}' already exists in table '{table}'"}
        
        # If all values are provided, validate foreign keys
        if all_values is not None:
           
            for i, col in enumerate(columns):
                col_name = col["name"]
                col_value = all_values[i]
                
                # Check unique constraints
                if col_name in table_data["constraints"].get("unique_key", []):
                    is_valid, error_message = validate_unique_key(database, table, col_name, col_value, table_data, columns, all_values)
                    if not is_valid:
                        return {"error": error_message}

                # Check foreign key constraints
                is_valid, error_message = validate_foreign_key(database, table_data, col_name, col_value)
                if not is_valid:
                    return {"error": error_message}
        
        # Create the document
        doc = {
            "_id": key,
            "value": values
        }
        
        # Insert into the main collection
        collection = get_db_collection(database, table)
        result = collection.insert_one(doc)

        if all_values is not None and columns is not None:
            column_dict = {}
            for i, col in enumerate(columns):
                column_dict[col["name"]] = all_values[i]
            
        # Update all indexes
        update_indexes(database, table, 'insert', key, column_dict)

        return {
            "message": f"Document inserted with ID {result.inserted_id}",
            "id": result.inserted_id
        }
    
    except errors.PyMongoError as e:
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        return {"error": f"Error inserting document: {str(e)}"}

def delete_document(database, table, key):

    try:
        # Load database metadata
        with open(get_metadata_file(database), 'r') as f:
            db_content = json.load(f)
        
        if table not in db_content.get("tables", {}):
            return {"error": f"Table '{table}' does not exist"}
        
        # Check if document exists
        collection = get_db_collection(database, table)
        document = collection.find_one({"_id": key})
        
        if not document:
            return {"error": f"Document with key '{key}' not found"}
        
        # Check if other tables reference this table (foreign key constraint)
        for other_table_name, other_table_data in db_content.get("tables", {}).items():
            if other_table_name == table:
                continue
                
            for fk in other_table_data.get("constraints", {}).get("foreign_keys", []):
                if fk["references"]["table"] == table:
                    # Check for references in the other table
                    other_collection = get_db_collection(database, other_table_name)
                    # Search for documents that might reference this key
                    for doc in other_collection.find():
                        values = doc["_id"].split("$")
                        if key in values:
                            return {
                                "error": f"Cannot delete: row is referenced by table '{other_table_name}'"
                            }
        
        table_data = db_content["tables"][table]
        values_dict = extract_values_to_dict(document, table_data)
        # Delete the document
        result = collection.delete_one({"_id": key})

        
        if result.deleted_count > 0:
            # Update all indexes
            update_indexes(database, table, 'delete', key, values_dict)
            
            return {
                "message": f"Document with key '{key}' deleted successfully",
                "deleted_count": result.deleted_count
            }
    
    
    except errors.PyMongoError as e:
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        return {"error": f"Error deleting document: {str(e)}"}