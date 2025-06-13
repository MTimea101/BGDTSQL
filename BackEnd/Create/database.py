import os
import json

#Folder and File
#META_DATA_FOLDER = "/Users/majercsiktimea/Desktop/Egyetem/NegyedikFelev/AB2/MiniABKR/BGDTSQL/bgdtsql"

META_DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../MetaData"))
os.makedirs(META_DATA_FOLDER, exist_ok=True)

def get_metadata_file(database):
    return os.path.join(META_DATA_FOLDER, f"{database}.json")

def create_database(database):
    file_path = get_metadata_file(database)
    
    if not os.path.exists(file_path):  #Create File if it does not exists 
        with open(file_path, "w") as f:
            json.dump({'tables':{}}, f, indent=4)

    return {"message": f"Database '{database}' created or already exists."}
