from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import json 

# Every function works, this connects correctly to Mongo

class MongoDB:
    """
    MongoDB is a wrapper of the main MongoClient functions. On instantiation it automatically connects using a URI, it handles creation and switch 
    of databases and collections. In order to write Mongo requires dictionaries or a list of dictionaries 
    (we will be using the latter in our application).  
    
    TODO pass bootstrap_servers, group_id, auto_offset_reset etc as environment parameters in the docker compose.
    """
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
        """
        It creates a db (think of it as a SQL DB), the default name is reviews since we will theoretically only need a single DB. It will then 
        append the newly created db to the list of dbs it is managing.

        Args:
            db_name: str

        Returns:
            self.db
        """

        print(f"Creating DB Called {db_name}", flush=True)
        self.db = self.client[db_name]
        self.db_list.append(db_name)
        return self.db

    def change_db(self, db_name = None):
        """
        Changes the db, if the wanted db does not exist it will create it and switch to it.

        Args:
            db_name: str

        Returns:
            self.db
        """

        if self.db is not None and self.db != db_name:
            if db_name not in self.db_list:
                self.create_db(db_name)
            self.db = db_name
            print(f"Switching to DB called {db_name}", flush=True)

        return self.db
        
    def create_collection(self, collection_name = None):
        """
        It creates a collection (think of it as a SQL table),
        the name of each collection is aligned with the name of the 
        kafka topic. 

        It will then append the newly created db to the list of dbs it is managing.   
        
        Args:
            collection_name: str

        Returns:
            self.collection_name
        """
        self.collection = self.db[collection_name]
        if collection_name not in self.collection_list:
            self.collection_list.append(collection_name)
        print(f"Creating a collection called {collection_name}", flush=True)
        return self.collection

    def change_collection(self, collection_name = None):
        """
        Changes the collection, if the wanted collection does not exist it will create it and switch to it.

        Args:
            collection_name: str

        Returns:
            self.collection: 
        """

        if self.collection is None or self.collection != collection_name:
            if collection_name not in self.collection_list:
                self.create_collection(collection_name)
            else:
                self.collection = self.db[collection_name]
            print(f"Switching to a collection called {collection_name}", flush=True)
        return self.collection
    
    def insert_single_dict(self, value: dict):
        """
        Using the current db and collection it writes a dictionary.
        """

        if type(value) != dict:
            print('Insert a dict object', flush=True)
            return 1 
        else:
            print(f'Inserting a dict:\n {value}', flush=True)
            self.collection.insert_one(value)

    def insert_list_dict(self, list_dict: list):
        """
        Using the current db and collection it writes a list of dictionaries.

        Args:
            list_dict: list of dictionaries

        """
        if type(list_dict) != list or len(list_dict) == 0 or type(list_dict[0]) != dict:
            return 1
        else:
            self.collection.insert_many(list_dict)
            print(f'Inserting a list of dictionaries:\n {list_dict}', flush=True)

    def read_all(self):
        """
        Reads every document (think of it as a SQL row) in the current collection.

        Returns:
            'pymongo.synchronous.cursor.Cursor' object  
        """

        results = self.collection.find()
        for result in results:
            print(f'{result}\n', flush=True)

        return results
    
    def write_on_mongo(self, msg, topic):
        try:
            if topic not in self.collection_list:
                # create_collection already switches to that collection
                self.create_collection(topic)
            # if the collection is not new, we switch to it to write correctly
            self.change_collection(topic)
            try:
                # Converts the string output of msg.value().decode('utf-8')) into a list of dict
                self.insert_list_dict(msg)
            # aggiusta gli errori con parquet
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON message: {str(e)}", flush=True)
            except Exception as e:
                print(f"Error processing message: {str(e)}", flush=True)
                
        except Exception as e:
            print(f"Error writing to MongoDB: {str(e)}", flush=True)