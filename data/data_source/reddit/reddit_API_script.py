import os
from pymongo import MongoClient
import praw
from dotenv import load_dotenv
from datetime import datetime, timezone
import logging


load_dotenv()

MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', 27017))
MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'data_pipeline_db')
MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')

try:
    client = MongoClient(
        host=MONGODB_HOST,
        port=MONGODB_PORT,
        username=MONGODB_USER,
        password=MONGODB_PASSWORD,
        authSource=MONGODB_DB_NAME,
        serverSelectionTimeoutMS=5000  
    )

    client.admin.command('ping')
    db = client[MONGODB_DB_NAME]
    reddit_collection = db['reddit_data']
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
    exit(1)

REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
REDDIT_PASSWORD = os.getenv('REDDIT_PASSWORD')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')  

try:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        password=REDDIT_PASSWORD,
        user_agent=REDDIT_USER_AGENT,  
        username=REDDIT_USERNAME
    )
    print(f"Logged in as: {reddit.user.me()}")
except Exception as e:
    print(f"Failed to authenticate with Reddit: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO)  
logger = logging.getLogger('prawcore')
logger.setLevel(logging.INFO)

def fetch_and_store_reddit_data():
    product_keywords = ['iphone', 'samsung', 'huawei', 'xiaomi', 'oneplus']
    subreddit_list = ['all']
    data = []

    for subreddit_name in subreddit_list:
        subreddit = reddit.subreddit(subreddit_name)
        for keyword in product_keywords:
            try:
                for submission in subreddit.search(keyword, limit=50):
                    submission_data = {
                        'reddit_id': submission.id,
                        'title': submission.title,
                        'author': str(submission.author),
                        'url': submission.url,
                        'selftext': submission.selftext,
                        'score': submission.score,
                        'num_comments': submission.num_comments,
                        'created_utc': datetime.fromtimestamp(submission.created_utc, tz=timezone.utc),
                        'keywords': [keyword],  
                        'subreddit': submission.subreddit.display_name  
                    }
                    data.append(submission_data)
            except Exception as e:
                logger.error(f'An error occurred while fetching submissions: {e}')
    
    for submission in data:
        logger.info(f"Title: {submission['title']} | Subreddit: {submission['subreddit']} | Keywords: {submission['keywords']}")
        reddit_collection.update_one(
        {'reddit_id': submission_data['reddit_id']},
        {'$set': submission_data},
        upsert=True
    )
    
    if data:
        try:
            result = reddit_collection.insert_many(data, ordered=False)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
        except Exception as e:
            logger.error(f"An error occurred while inserting data into MongoDB: {e}")
    else:
        logger.info("No data to insert into MongoDB")

if __name__ == "__main__":
    fetch_and_store_reddit_data()
