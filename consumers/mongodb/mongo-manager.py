from pymongo import MongoClient
from bson.objectid import ObjectId
import confluent_kafka
from datetime import datetime

class MongoController:
    def __init__(self, host='localhost', port=27017):
        self.client = MongoClient(f'mongodb://{host}:{port}')
        self.db_list = []
        self.db = 0
        self.collection_list = []
        self.collection = 0
        self.user_schema = {
            "_id": ObjectId(),
            "source": str,
            "text": str,
            "date": datetime,
            "yt-video-id": str,
            "yt-likes": int,
            "yt-reply-count": int,
            "tp-stars": int,
            "tp-location": str,
            "re-vote": int,
            "re-reply-count": int 
        }
        
    def create_db(self, db_name = 'reviews'):
        self.db = self.client[db_name]
        self.db_list.append(db_name)
        return self.db

    def change_db(self, db_name = None):
        if self.db is not None and self.db != db_name:
            if db_name not in self.db_list:
                self.create_db(db_name)
            self.db = db_name
        return self.db
        
    def create_collection(self, collection_name = None):
        self.collection = self.db[collection_name]
        self.collection_list.append(collection_name)
        return self.collection

    def change_collection(self, collection_name = None):
        if self.collection is not None and self.collection != collection_name:
            if collection_name not in self.collection:
                self.create_collection(collection_name)
            self.collection = collection_name
        return self.collection
    
    def insert_single_dict(self, value: dict):
        if type(value) != dict:
            return 'Insert a dict object'
        else:
            self.collection.insert_one(value)

    def insert_list_dict(self, list_dict: list):
        if type(list_dict) != list or len(list_dict) == 0 or type(list_dict[0]) != dict:
            # raise typerror
            return 'You did not input a list or the elements of the list are not dictionaries or the list is empty'
        else:
            self.collection.insert_many(list_dict)

"""
        Youtube
        extracted_comment = {
            "source": "youtube",
            "text": comment.get("textOriginal", None),
            "date": comment.get("publishedAt", None),
            "yt-video-id": video,
            "yt-likes": comment.get("likeCount", None),
            "yt-reply-count": item["snippet"].get("totalReplyCount", 0)
        }

Trustpilot
        full_review["source"] = "trustpilot"
        full_review["text"] = text[num_review]
        full_review["date"] = dates[num_review]
        full_review["tp-stars"] = ratings[num_review]["data-service-review-rating"]
        full_review["tp-Location"] = locations[num_review].get_text()

Reddit

        submission_data = {
            'source': 'reddit'
            'text': submission.selftext,
            'date': datetime.fromtimestamp(submission.created_utc, tz=timezone.utc),
            're-vote': submission.score,
            're-reply-count': submission.num_comments,
        }
"""
