from pymongo import MongoClient
from bson.objectid import ObjectId
import confluent_kafka
from datetime import datetime

# Every function works, this connects correctly to Mongo

class MongoManager:
    def __init__(self, host='mongo', port=27017):
        self.client = MongoClient(f'mongodb://{host}:{port}')
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
        
    def create_db(self, db_name = 'reviews'):
        print(f"Creating DB Called {db_name}")
        self.db = self.client[db_name]
        self.db_list.append(db_name)
        return self.db

    def change_db(self, db_name = None):
        if self.db is not None and self.db != db_name:
            if db_name not in self.db_list:
                self.create_db(db_name)
            self.db = db_name
            print(f"Switching to DB called {db_name}")

        return self.db
        
    def create_collection(self, collection_name = None):
        self.collection = self.db[collection_name]
        if collection_name not in self.collection_list:
            self.collection_list.append(collection_name)
        print(f"Creating a collection called {collection_name}")
        return self.collection

    def change_collection(self, collection_name = None):
        if self.collection is None or self.collection != collection_name:
            if collection_name not in self.collection_list:
                self.create_collection(collection_name)
            else:
                self.collection = self.db[collection_name]
            print(f"Switching to a collection called {collection_name}")
        return self.collection
    
    def insert_single_dict(self, value: dict):
        if type(value) != dict:
            print('Insert a dict object')
            return 1 
        else:
            print(f'Inserting a dict:\n {value}')
            self.collection.insert_one(value)

    def insert_list_dict(self, list_dict: list):
        if type(list_dict) != list or len(list_dict) == 0 or type(list_dict[0]) != dict:
            return 1
        else:
            self.collection.insert_many(list_dict)
            print(f'Inserting a list of dictionaries:\n {list_dict}')

    def read_all(self):
        results = self.collection.find()
        for result in results:
            print(f'{result}\n')

        return results
