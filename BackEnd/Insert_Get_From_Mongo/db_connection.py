import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

# Get URI from the environment
MONGODB_ATLAS_URI = os.getenv("MONGODB_ATLAS_URI")

# Connect to MongoDB
client = MongoClient(MONGODB_ATLAS_URI)

def get_db_collection(database, table):
    db = client[database]
    collection = db[table]
    return collection
