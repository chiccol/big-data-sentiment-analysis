from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDB:
    def __init__(self, host='mongo', port=27017):
        try:
            self.client = MongoClient(f'mongodb://{host}:{port}')
            logging.info(f"Connected to MongoDB at {host}:{port}")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")
            raise e
        self.db_list = []
        self.db = None
        self.collection_list = []
        self.collection = None
        self.user_schema = {
            "_id": ObjectId(),
            "source": str,
            "text": str,
            "date": datetime,
            "yt-video-id": str,
            "yt-like-count": int,
            "yt-reply-count": int,
            "tp-stars": int,
            "tp-location": str,
            "re-vote": int,
            "re-reply-count": int 
        }
        
    def create_db(self, db_name='reviews'):
        try:
            logging.info(f"Creating database: {db_name}")
            self.db = self.client[db_name]
            self.db_list.append(db_name)
            # Ensure the database exists by inserting a dummy document
            self.db['dummy_collection'].insert_one({"status": "created"})
            logging.info(f"Database '{db_name}' created successfully with a dummy document.")
            return self.db
        except Exception as e:
            logging.error(f"Error creating database '{db_name}': {e}")
            raise e

    def create_collection(self, collection_name=None):
        try:
            logging.info(f"Creating collection: {collection_name}")
            self.collection = self.db[collection_name]
            if collection_name not in self.collection_list:
                self.collection_list.append(collection_name)
            logging.info(f"Collection '{collection_name}' created successfully.")
            return self.collection
        except Exception as e:
            logging.error(f"Error creating collection '{collection_name}': {e}")
            raise e

    def insert_single_dict(self, value: dict):
        try:
            if not isinstance(value, dict):
                logging.error("Input must be a dictionary.")
                return 1
            logging.info(f"Inserting document: {value}")
            self.collection.insert_one(value)
            logging.info("Document inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting document: {e}")

    def insert_list_dict(self, list_dict: list):
        try:
            if not isinstance(list_dict, list) or not list_dict or not isinstance(list_dict[0], dict):
                logging.error("Input must be a list of dictionaries.")
                return 1
            logging.info(f"Inserting list of documents: {list_dict}")
            self.collection.insert_many(list_dict)
            logging.info("List of documents inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting list of documents: {e}")

    def read_all(self):
        try:
            logging.info("Reading all documents from the current collection.")
            results = self.collection.find()
            for result in results:
                logging.info(f"Document: {result}")
            return results
        except Exception as e:
            logging.error(f"Error reading documents: {e}")


