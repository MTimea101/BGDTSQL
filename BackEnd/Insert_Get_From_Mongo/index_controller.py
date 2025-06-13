import json
from pymongo import errors
from BackEnd.Insert_Get_From_Mongo.db_connection import client, get_db_collection
from BackEnd.Create.database import get_metadata_file

def extract_values_to_dict(document, table_data):
    result = {}
    
    # Extract primary key parts
    primary_keys = table_data["constraints"]["primary_key"]
    if len(primary_keys) > 1:
        # Composite primary key
        pk_parts = document["_id"].split("$")
        for i, pk in enumerate(primary_keys):
            if i < len(pk_parts):
                result[pk] = pk_parts[i]
    else:
        # Single primary key
        result[primary_keys[0]] = document["_id"]
    
    # Extract non-primary key parts
    non_pk_columns = [col["name"] for col in table_data["columns"] 
                     if col["name"] not in primary_keys]
    
    if document.get("value"):
        values = document["value"].split("#")
        for i, col in enumerate(non_pk_columns):
            if i < len(values):
                result[col] = values[i]
    
    return result

def create_mongodb_index(database, table_name, index_name, columns, is_unique=False):
    try:
        # Connect to MongoDB
        db = client[database]
        
        # Get the main table collection to read existing data
        main_collection = db[table_name]
        
        # Create a collection name for the index
        index_collection_name = f"{table_name}_{index_name}_ind"
        
        # Create or get the index collection
        index_collection = db[index_collection_name]
        
        # Read database metadata to get primary key info
        with open(get_metadata_file(database), 'r') as f:
            db_content = json.load(f)
        
        table_data = db_content["tables"][table_name]
        all_documents = list(main_collection.find())
        
        # Process each document to build the index
        for doc in all_documents:
            primary_key = doc["_id"]
            
            # Extract values to a dictionary
            values_dict = extract_values_to_dict(doc, table_data)
            
            # Use the update_indexes function to add this document to the index
            update_result = update_indexes(database, table_name, 'insert', primary_key, values_dict, specific_index=index_name, specific_columns=columns, is_unique=is_unique)
            
            if "error" in update_result:
                # Clean up on error
                index_collection.delete_many({})
                return update_result
        
        return {"message": f"Index {index_name} created successfully on {table_name}"}
    
    except errors.PyMongoError as e:
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        return {"error": f"Error creating index: {str(e)}"}
    

def update_indexes(database, table_name, operation, primary_key, values, old_values=None, specific_index=None, specific_columns=None, is_unique=None):
    try:
        # Load table metadata
        with open(get_metadata_file(database), 'r') as f:
            db_content = json.load(f)
        
        if table_name not in db_content.get("tables", {}):
            return {"error": f"Table '{table_name}' does not exist"}
        
        table_data = db_content["tables"][table_name]
        
        # Connect to MongoDB
        db = client[database]
        
        # If updating specific index
        if specific_index:
            if not specific_columns:
                return {"error": "Must provide columns for specific index"}
            
            # Process just this one index
            index_collection_name = f"{table_name}_{specific_index}_ind"
            index_collection = db[index_collection_name]
            
            # Build the index key
            if len(specific_columns) > 1:
                # Composite index
                index_key_parts = []
                for col in specific_columns:
                    if col not in values:
                        return {"error": f"Missing value for column {col}"}
                    index_key_parts.append(values[col])
                index_key = "$".join(index_key_parts)
            else:
                # Single column index
                col = specific_columns[0]
                if col not in values:
                    return {"error": f"Missing value for column {col}"}
                index_key = values[col]
            
            # Handle the operation
            if operation == 'insert':
                if is_unique:
                    # For unique index, check if key already exists
                    existing = index_collection.find_one({"_id": index_key})
                    if existing:
                        return {"error": f"Unique constraint violation in index {specific_index}"}
                    
                    # Insert into index collection
                    index_collection.insert_one({
                        "_id": index_key,
                        "value": primary_key
                    })
                else:
                    # For non-unique index, append to existing or create new
                    existing = index_collection.find_one({"_id": index_key})
                    if existing:
                        # Check if this primary key is already in the list
                        existing_values = existing["value"].split("#")
                        if primary_key not in existing_values:
                            # Append to existing value
                            updated_value = existing["value"] + "#" + primary_key
                            index_collection.update_one(
                                {"_id": index_key},
                                {"$set": {"value": updated_value}}
                            )
                    else:
                        # Create new entry
                        index_collection.insert_one({
                            "_id": index_key,
                            "value": primary_key
                        })
            
            elif operation == 'delete':
                # Find the entry in the index
                existing = index_collection.find_one({"_id": index_key})
                if existing:
                    if is_unique:
                        # For unique index, delete the entry
                        index_collection.delete_one({"_id": index_key})
                    else:
                        # For non-unique index, remove the primary key from the list
                        existing_values = existing["value"].split("#")
                        if primary_key in existing_values:
                            existing_values.remove(primary_key)
                            
                            if not existing_values:
                                # If no values left, delete the entry
                                index_collection.delete_one({"_id": index_key})
                            else:
                                # Update with remaining values
                                updated_value = "#".join(existing_values)
                                index_collection.update_one(
                                    {"_id": index_key},
                                    {"$set": {"value": updated_value}}
                                )
            
            return {"message": f"Index {specific_index} updated successfully"}  
        else:

            # Get indexes from metadata
            indexes = table_data.get("indexes", [])
            
            # Also include primary key and unique key constraints as indexes
            pk_columns = table_data["constraints"]["primary_key"]
            unique_columns = table_data["constraints"].get("unique_key", [])
            
            # Process each index
            for index in indexes:
                index_name = index["name"]
                index_columns = index["columns"]
                
                # Determine if this is a unique index
                is_unique = any(col in unique_columns for col in index_columns)
                
                # Get index collection
                index_collection_name = f"{table_name}_{index_name}_ind"
                index_collection = db[index_collection_name]
                
                # Build the index key
                if len(index_columns) > 1:
                    # Composite index
                    index_key_parts = []
                    for col in index_columns:
                        if values and col in values:
                            index_key_parts.append(values[col])
                        elif old_values and col in old_values:
                            index_key_parts.append(old_values[col])
                        else:
                            # Cannot build complete index key
                            return {"error": f"Cannot build index key for {index_name}: missing column {col}"}
                    
                    index_key = "$".join(index_key_parts)
                else:
                    # Single column index
                    col = index_columns[0]
                    if values and col in values:
                        index_key = values[col]
                    elif old_values and col in old_values:
                        index_key = old_values[col]
                    else:
                        # Cannot build index key
                        return {"error": f"Cannot build index key for {index_name}: missing column {col}"}
                
                # Handle the operation
                if operation == 'insert':
                    if is_unique:
                        # For unique index, check if key already exists
                        existing = index_collection.find_one({"_id": index_key})
                        if existing:
                            return {"error": f"Unique constraint violation in index {index_name}"}
                        
                        # Insert into index collection
                        index_collection.insert_one({
                            "_id": index_key,
                            "value": primary_key
                        })
                    else:
                        # For non-unique index, append to existing or create new
                        existing = index_collection.find_one({"_id": index_key})
                        if existing:
                            # Check if this primary key is already in the list
                            existing_values = existing["value"].split("#")
                            if primary_key not in existing_values:
                                # Append to existing value
                                updated_value = existing["value"] + "#" + primary_key
                                index_collection.update_one(
                                    {"_id": index_key},
                                    {"$set": {"value": updated_value}}
                                )
                        else:
                            # Create new entry
                            index_collection.insert_one({
                                "_id": index_key,
                                "value": primary_key
                            })
                
                elif operation == 'delete':
                    # Find the entry in the index
                    existing = index_collection.find_one({"_id": index_key})
                    if existing:
                        if is_unique:
                            # For unique index, delete the entry
                            index_collection.delete_one({"_id": index_key})
                        else:
                            # For non-unique index, remove the primary key from the list
                            existing_values = existing["value"].split("#")
                            if primary_key in existing_values:
                                existing_values.remove(primary_key)
                                
                                if not existing_values:
                                    # If no values left, delete the entry
                                    index_collection.delete_one({"_id": index_key})
                                else:
                                    # Update with remaining values
                                    updated_value = "#".join(existing_values)
                                    index_collection.update_one(
                                        {"_id": index_key},
                                        {"$set": {"value": updated_value}}
                                    )
                                    
        return {"message": f"Indexes updated successfully for {operation} operation"}
    except errors.PyMongoError as e:
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        return {"error": f"Error updating indexes: {str(e)}"}