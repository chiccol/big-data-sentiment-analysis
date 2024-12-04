import os
import praw
from dotenv import load_dotenv
from datetime import datetime, timezone
import json
from kafka_producer import KafkaProducer
from time import sleep
import logging
import time 
import json
from datetime import datetime
from time import sleep



def getcomments_reddit(
    submission_id,
    reddit_client,
    from_date,
    company,
    max_num_comments,
    producer,
    record_list=[],
    save_submission=True):
    """
    Fetch comments from a Reddit submission and send them to a Kafka topic, with an option to save submission data.

    Parameters:
    - submission_id: str, the Reddit submission ID
    - reddit_client: praw.Reddit, an authenticated Reddit client instance
    - from_date: str, the date from which to start fetching comments (ISO 8601 format)
    - company: str, the name of the company (used as the Kafka topic)
    - max_num_comments: int, the maximum number of comments to fetch
    - producer: KafkaProducer, the Kafka producer to send the comments to a Kafka topic
    - save_submission: bool, whether to save the submission data (default is True)
    """
    logging.debug(f"Entered getcomments_reddit with submission_id={submission_id}, company={company}")

    
    # Retrieve the submission
    try:
        submission = reddit_client.submission(id=submission_id)
        logging.info(f"Fetched submission {submission_id} for company '{company}'")
    except Exception as e:
        logging.exception(f"Error fetching submission {submission_id} for company '{company}': {e}")
        return None

    if save_submission:
        # Fetch submission data
        submission_data = {
            "id": submission_id,
            "source": "reddit",
            "text": submission.title + ": " + submission.selftext,
            "date": datetime.fromtimestamp(submission.created_utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "re-subreddit": str(submission.subreddit),
            "re-vote": submission.score,
            "re-reply-count": submission.num_comments
        }
        # Append submission data to record_list
        record_list.append(submission_data)

        # If record_list reaches 100 items, send to Kafka
        if len(record_list) >= 100:
            records_json = json.dumps(record_list).encode('utf-8')
            try:
                producer.produce(record=records_json, topic=company)
                logging.info(f"Sent batch of {len(record_list)} records to Kafka topic '{company}'")
                record_list = []  # Reset the list after sending
            except Exception as e:
                logging.exception(f"Error sending records to Kafka for submission {submission_id} of company '{company}': {e}")
    else:
        logging.info(f"Skipping submission data for submission {submission_id} of company '{company}'")


    # Now process comments
    try:
        submission.comments.replace_more(limit=None)
        comments_list = submission.comments.list()
        logging.debug(f"Retrieved {len(comments_list)} comments for submission {submission_id}")
    except Exception as e:
        logging.exception(f"Error processing comments for submission {submission_id} of company '{company}': {e}")
        return None

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    num_comments = 0
    comments_to_send = []

    # Sort comments by creation time (oldest first)
    comments_list.sort(key=lambda x: x.created_utc)
    logging.debug(f"Sorted comments for submission {submission_id} by creation time")

    last_comment_id = None

    for comment in comments_list:
        comment_datetime = datetime.fromtimestamp(comment.created_utc)
        comment_date_str = comment_datetime.strftime(date_format)

        if from_date:
            try:
                from_date_dt = datetime.strptime(from_date, date_format)
            except ValueError as ve:
                logging.error(f"Invalid from_date format: {from_date}. Expected format {date_format}")
                break  # Exit the loop as from_date is invalid

            if comment_datetime < from_date_dt:
                logging.debug(f"Comment {comment.id} is older than {from_date}. Skipping.")
                continue

        extracted_comment = {
            "id": comment.id,
            "source": "reddit",
            "text": comment.body,
            "date": comment_date_str,
            "re-subreddit": str(submission.subreddit),
            "re-vote": comment.score,
            "re-reply-count": len(comment.replies)
        }
        record_list.append(extracted_comment)
        num_comments += 1
        
         # If record_list reaches 100 items, send to Kafka
        if len(record_list) >= 100:
            records_json = json.dumps(record_list).encode('utf-8')
            try:
                producer.produce(record=records_json, topic=company)
                logging.info(f"Sent batch of {len(record_list)} records to Kafka topic '{company}'")
                record_list = []  # Reset the list after sending
            except Exception as e:
                logging.exception(f"Error sending records to Kafka for submission {submission_id} of company '{company}': {e}")

        if num_comments >= max_num_comments:
            logging.info(f"Reached {max_num_comments} comments for submission {submission_id} of company '{company}'")
            last_comment_id = comment.id
            break

    if not last_comment_id and comments_list:
        last_comment_id = comments_list[-1].id
        logging.info(f"Comment {last_comment_id} is the last one of submission {submission_id} at date {comment_date_str}")

    if comments_to_send:
        comments_json = json.dumps(comments_to_send).encode('utf-8')
        try:
            producer.produce(record=comments_json, topic=company)
            logging.info(f"Sent {num_comments} comments for submission {submission_id} of company '{company}' to Kafka topic '{company}'")
        except Exception as e:
            logging.exception(f"Error sending comments to Kafka for submission {submission_id} of company '{company}': {e}")
    else:
        logging.info(f"No new comments to fetch for submission {submission_id} of company '{company}'")
        last_comment_id = None

    sleep(5)
    logging.debug(f"Exiting getcomments_reddit for submission_id={submission_id}, company={company} with last_comment_id={last_comment_id}")
    return last_comment_id, record_list


def search_posts(
    query,
    after_date,
    reddit_client,
    company,
    max_posts,
    subreddit_list=None
):
    """
    Search for Reddit posts matching the query and return a list of submission details.
    
    Returns:
    - new_posts: dict, mapping submission_id to its details ('from_date', 'max_num_comments')
    - reddit_companies_posts: dict, updated posts data
    """
    logging.info(f"Entered search_posts with company='{company}', query='{query}', after_date='{after_date}', max_posts={max_posts}")
    num_posts = 0
    reddit_companies_posts_path = "reddit_companies.json"
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    new_posts = {}

    # Load existing posts data
    try:
        with open(reddit_companies_posts_path, 'r') as file:
            reddit_companies_posts = json.load(file)
        logging.debug(f"Loaded existing posts data from '{reddit_companies_posts_path}'")
    except FileNotFoundError:
        reddit_companies_posts = {}
        logging.warning(f"File '{reddit_companies_posts_path}' not found. Starting with empty posts data.")

    if company not in reddit_companies_posts:
        reddit_companies_posts[company] = {
            "posts": {},
            "from_date": "2023-01-01T00:00:00Z",
            "query": query,
            "subreddits": subreddit_list if subreddit_list is not None else [company],
            "max_posts": max_posts,
        }
        logging.info(f"Initialized posts data for company '{company}'")

    # Define subreddits to search in
    subreddits = '+'.join(subreddit_list) if subreddit_list is not None else 'all'
    logging.info(f"Searching in subreddits '{subreddits}' with query '{query}' for company '{company}'")

    # Use Reddit's API to search for posts
    try:
        search_start_time = time.time()
        search_results = reddit_client.subreddit(subreddits).search(
            query=query,
            sort='new',
            limit=None  
        )
        logging.debug(f"Initiated search on Reddit for company '{company}'")
    except Exception as e:
        logging.exception(f"Error initiating search on Reddit for company '{company}': {e}")
        return {}, reddit_companies_posts

    # Convert search_results to list to count and iterate
    try:
        search_results_list = list(search_results)
        logging.info(f"Found {len(search_results_list)} total search results for company '{company}'")
    except Exception as e:
        logging.exception(f"Error converting search results to list for company '{company}': {e}")
        return {}, reddit_companies_posts

    for submission in search_results_list:
        logging.debug(f"Processing post {submission.id} for company '{company}'")
        
        if num_posts >= max_posts:
            logging.info(f"Reached the maximum number of posts ({max_posts}) for company '{company}'")
            break

        submission_created = datetime.fromtimestamp(submission.created_utc)

        # Skip if the post is before the after_date
        try:
            after_date_dt = datetime.strptime(after_date, date_format)
        except ValueError as ve:
            logging.error(f"Invalid after_date format: {after_date}. Expected format {date_format}")
            break  # Exit the loop as after_date is invalid

        if submission_created < after_date_dt:
            logging.debug(f"Post {submission.id} is older than {after_date}. Skipping.")
            continue

        # Check if the post is already in the data
        existing_posts = list(reddit_companies_posts[company]["posts"].keys())
        if submission.id in existing_posts:
            logging.debug(f"Post {submission.id} is already processed for company '{company}'. Skipping.")
            continue

        # Add the post to the data
        reddit_companies_posts[company]["posts"][submission.id] = {
            "from_date": after_date,
            "max_num_comments": 10
        }
        
        new_posts[submission.id] = {    
            "from_date": after_date,
            "max_num_comments": 10  
        }
        num_posts += 1
        logging.info(f"Added post {submission.id} to company '{company}'")

        sleep(3)  

    if num_posts == 0:
        logging.info(f"No posts found for company '{company}', new posts will be searched from now")
        reddit_companies_posts[company]["from_date"] = datetime.now().strftime(date_format)
    
    # Save the updated data
    try:
        with open(reddit_companies_posts_path, 'w') as file:
            json.dump(reddit_companies_posts, file, indent=4)
        logging.info(f"Saved updated posts data to '{reddit_companies_posts_path}' for company '{company}'")
    except Exception as e:
        logging.exception(f"Error saving updated posts data to '{reddit_companies_posts_path}' for company '{company}': {e}")

    search_end_time = time.time()
    logging.info(f"Completed search_posts for company '{company}' in {search_end_time - search_start_time:.2f} seconds")

    logging.debug(f"Exiting search_posts for company '{company}' with {num_posts} new posts found")
    return new_posts, reddit_companies_posts