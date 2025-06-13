from BackEnd.Insert_Get_From_Mongo.db_connection import get_db_collection

def load_index(database, table, name):

    index_collection_name = f"{table}_{name}_ind"
    try:
        collection = get_db_collection(database, index_collection_name)
        index_entries = collection.find()
        index_map = {}
        for doc in index_entries:
            value = doc.get("value", "")
            doc_ids = value.split("#") if value else []
            index_map[doc["_id"]] = doc_ids
        return index_map
    except Exception as e:
        print(f"Error loading index from MongoDB for {name}: {e}")
        return None

def get_matching_ids_from_index(index_data, operator, target):

    matching_ids = set()
    for key, id_list in index_data.items():
        try:
            k = float(key)
            t = float(target)
        except ValueError:
            k = key
            t = target

        if operator == "=" and k == t:
            matching_ids.update(id_list)
        elif operator == ">" and k > t:
            matching_ids.update(id_list)
        elif operator == "<" and k < t:
            matching_ids.update(id_list)
        elif operator == ">=" and k >= t:
            matching_ids.update(id_list)
        elif operator == "<=" and k <= t:
            matching_ids.update(id_list)

    return matching_ids
