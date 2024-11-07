from pymongo import MongoClient, ObjectId
from bson.objectid import ObjectId
import confluent_kafka
from datetime import datetime

class MongoController:
    def __init__(self, host='mongo', port=27017):
        self.client = MongoClient(f'mongodb://{host}:{port}')
        self.db_names = None
        self.db = None
        self.collection_names = None
        self.collection = None
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
