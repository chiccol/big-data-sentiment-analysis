import os
import json
from time import sleep
from datetime import datetime
from dotenv import load_dotenv
import praw
from kafka_producer import KafkaProducer
from reddit import getcomments_reddit, search_posts, encode_message_to_parquet
import logging
from config import CONFIG

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("reddit-producer")

logger.info("Started logging")
if __name__ == "__main__":
    sleep(10)
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "reddit.env"))
    logger.info(f"Current working directory: {os.getcwd()}")

    # Load Reddit credentials from environment variables
    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_username = os.getenv("REDDIT_USERNAME")
    reddit_user_agent = os.getenv("REDDIT_USER_AGENT")
    reddit_user_password = os.getenv("REDDIT_USER_PASSWORD")
    logger.info(f"Reddit client ID: {reddit_client_id}")
    
    # Initialize Reddit Scraper
    try:
        reddit_scraper = praw.Reddit(
            client_id=reddit_client_id,
            client_secret=reddit_client_secret,
            user_agent=reddit_user_agent,
            username=reddit_username,
            password=reddit_user_password
        )
        logger.info(f"Logged in as: {reddit_scraper.user.me()}")
    except Exception as e:
        logger.error(f"Error initializing Reddit scraper: {e}")
        exit(1)

    # Initialize Kafka Producer
    try:
        client_id = CONFIG["client_id"]
        bootstrap_servers = CONFIG["bootstrap_servers"] # Kafka broker
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=client_id)
        logger.info(f"Kafka producer '{client_id}' connected to '{bootstrap_servers}'")
    except Exception as e:
        logger.error(f"Error initializing Kafka Producer: {e}")
        exit(1)

    # Load companies and scraping info
    companies_path = CONFIG["companies_post_path"]
    try:
        with open(companies_path, 'r') as file:
            companies = json.load(file)
        logger.info(f"Loaded Reddit submissions data from {companies_path}")
    except FileNotFoundError:
        companies = {}
        logger.warning(f"{companies_path} not found. Starting with empty Reddit submissions data.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {companies_path}: {e}")
        companies = {}

    date_format = CONFIG["date_format"]

    while True:
        for company in companies.keys():
            logger.info(f"Searching for new submissions for '{company}'")
            try:
                new_submissions, companies_submissions = search_posts(
                    query=companies[company].get("search_query", ""),
                    after_date=companies[company].get("search_from_date", "2023-01-01T00:00:00Z"),
                    comments_after_date=companies[company].get("get_comments_from_date", "2023-01-01T00:00:00Z"),
                    reddit_client=reddit_scraper,
                    company=company,
                    max_posts=companies[company].get("max_submissions", 10),
                    subreddit_list=companies[company].get("subreddits", [company])
                )
                logger.info(f"Found {len(new_submissions)} new submissions for '{company}'")
            except Exception as e:
                logger.error(f"Error during search_posts for '{company}': {e}")
                continue

            # initialize the list for sending messages to Kafka
            record_list = []
            
            for submission_id in new_submissions.keys():
                from_date = companies_submissions[company]["posts"][submission_id].get("from_date")
                max_num_comments = companies_submissions[company]["posts"][submission_id].get("max_num_comments", 100)

                if not submission_id or not from_date:
                    logger.warning(f"Invalid submission data for submission ID '{submission_id}'. Skipping.")
                    continue

                logger.info(f"Fetching comments for submission '{submission_id}' of company '{company}'")
                try:
                    last_comment_id, record_list = getcomments_reddit(
                        submission_id=submission_id,
                        reddit_client=reddit_scraper,
                        from_date=from_date,
                        company=company,
                        max_num_comments=max_num_comments,
                        producer=producer,
                        record_list=record_list,
                        save_submission=True
                    )
                    logger.info(f"Fetched comments for submission '{submission_id}', last comment ID: '{last_comment_id}'")
                except Exception as e:
                    logger.error(f"Error fetching Reddit comments for submission '{submission_id}': {e}")
                    continue

                # Update the submission's from_date to the last fetched comment's date
                if not last_comment_id:
                    companies_submissions[company]["posts"][submission_id]["from_date"] = datetime.now().strftime(date_format)
                    logger.info(f"Updated 'from_date' for submission '{submission_id}' of company '{company}'")
                    
            # send all the remaining comments in the record_list to Kafka
            if len(record_list) > 0:
                record_serialized = encode_message_to_parquet(record_list) 
                try:
                    producer.produce(record=record_serialized, topic=company)
                    logger.info(f"Sent batch of {len(record_list)} records to Kafka topic '{company}'")
                    record_list = []  # Reset the list after sending
                except Exception as e:
                    logger.exception(f"Error sending records to Kafka for submission {submission_id} of company '{company}': {e}")

            # Save the updated Reddit submissions data
            try:
                with open(companies_path, 'w') as file:
                    json.dump(companies_submissions, file, indent=4)
                logger.info(f"Updated Reddit submissions data saved to '{companies_path}'")
            except Exception as e:
                logger.error(f"Error saving Reddit submissions data: {e}")

        # Sleep before the next iteration
        logger.info("Sleeping for 60 seconds.")
        sleep(60)
