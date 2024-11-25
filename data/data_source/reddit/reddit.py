import os
import praw
from dotenv import load_dotenv
from datetime import datetime, timezone
import json
from kafka_producer import KafkaProducer
from time import sleep



def getcomments_reddit(
    submission_id,
    reddit_client,
    from_date,
    company,
    max_num_comments,
    producer,
    save_submission=True,
    after_comment_id=None
):
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
    - after_comment_id: str, the ID of the last comment fetched (used for pagination)
    """

    # Retrieve the submission
    try:
        submission = reddit_client.submission(id=submission_id)
    except Exception as e:
        print(f"Error fetching submission {submission_id}: {e}")
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

        # Send submission data to Kafka
        submission_json = json.dumps(submission_data).encode('utf-8')
        try:
            producer.produce(record=submission_json, topic=company)
            print(f"Sent submission data for submission {submission_id} of company {company}")
        except Exception as e:
            print(f"Error sending submission data to Kafka for submission {submission_id}: {e}")
    else:
        print(f"Skipping submission data for submission {submission_id} of company {company}")

    # Now process comments
    submission.comments.replace_more(limit=None)
    comments_list = submission.comments.list()
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    num_comments = 0
    comments_to_send = []

    # Sort comments by creation time (newest first)
    comments_list.sort(key=lambda x: x.created_utc, reverse=True)

    # If after_comment_id is provided, find the index to start from
    start_index = 0
    if after_comment_id:
        for idx, comment in enumerate(comments_list):
            if comment.id == after_comment_id:
                start_index = idx + 1
                break

    for comment in comments_list[start_index:]:
        comment_datetime = datetime.fromtimestamp(comment.created_utc)
        comment_date_str = comment_datetime.strftime(date_format)

        if comment_datetime < datetime.strptime(from_date, date_format):
            print(f"Comment is older than {from_date}")
            break

        extracted_comment = {
            # "id": submission_id,
            "id": comment.id,
            "source": "reddit",
            "text": comment.body,
            "date": comment_date_str,
            "re-subreddit": str(submission.subreddit),
            "re-vote": comment.score,
            "re-reply-count": len(comment.replies)#,
            # "author": str(comment.author),
            # "parentId": comment.parent_id,
            # "permalink": comment.permalink
        }
        comments_to_send.append(extracted_comment)
        num_comments += 1

        if num_comments >= max_num_comments:
            print(f"Reached {max_num_comments} comments for submission {submission_id} of company {company}")
            after_comment_id = comment.id
            break

    if comments_to_send:
        comments_json = json.dumps(comments_to_send).encode('utf-8')
        try:
            producer.produce(record=comments_json, topic=company)
            print(f"Sent {num_comments} comments for submission {submission_id} of company {company}")
        except Exception as e:
            print(f"Error sending comments to Kafka for submission {submission_id}: {e}")
    else:
        print(f"No new comments to fetch for submission {submission_id} of company {company}")
        after_comment_id = None

    sleep(5)  
    return after_comment_id


import praw
import json
from datetime import datetime
from time import sleep

def search_posts(
    query,
    after_date,
    reddit_client,
    company,
    max_posts,
    min_comments=10,
    min_score=100,
    subreddit_list=None
):
    """
    Search for Reddit posts matching the query and return a list of submission details.
    
    Returns:
    - new_posts: list of dicts, each containing 'submission_id', 'from_date', 'max_num_comments'
    - reddit_companies_posts: updated posts data
    """
    num_posts = 0
    reddit_companies_posts_path = "reddit_companies.json"
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    new_posts = []

    # Load existing posts data
    try:
        with open(reddit_companies_posts_path, 'r') as file:
            reddit_companies_posts = json.load(file)
    except FileNotFoundError:
        reddit_companies_posts = {}

    if company not in reddit_companies_posts:
        reddit_companies_posts[company] = {
            # "posts": {},
            "from_date": after_date,
            "query": query,
            "submissions": [],
            "subreddits": [company]
        }

    # Define subreddits to search in
    subreddits = '+'.join(subreddit_list) if subreddit_list is not None else 'all'
    print(f"Searching in subreddits {subreddits} with query {query}")

    # Use Reddit's API to search for posts
    search_results = reddit_client.subreddit(subreddits).search(
        query=query,
        sort='new',
        limit=None  
    )
    # print(f"Search result length {len(list(search_results))} for company {company}")

    for submission in search_results:
        print(f"Processing post {submission.id} for {company}")
        
        if num_posts >= max_posts:
            print("Reached the maximum number of posts.")
            break

        submission_created = datetime.fromtimestamp(submission.created_utc)

        # Skip if the post is before the after_date
        if submission_created < datetime.strptime(after_date, date_format):
            print(f"Post {submission.id} is older than {after_date}")
            continue

        # # Check if the post meets the minimum criteria
        # if submission.num_comments < min_comments:
        #     print(f"Post {submission.id} does not have enough comments.")
        #     continue

        # Check if the post is already in the data
        if submission.id in [sub["submission_id"] for sub in reddit_companies_posts[company]["submissions"]]:
            print(f"Post {submission.id} is already processed.")
            continue

        # Add the post to the data
        # reddit_companies_posts[company]["posts"][submission.id] = {
        #     "_id": submission.id,
        #     "source": "reddit",
        #     "text": submission.title + " : " + submission.selftext,
        #     "date": submission_created.strftime(date_format),
        #     "re-subreddit": str(submission.subreddit),
        #     "re-vote": submission.score,
        #     "re-reply-count": submission.num_comments
        # }
        new_posts.append({
            "submission_id": submission.id,
            "from_date": after_date,
            "max_num_comments": 10  # Fetch up to n comments per post for every iteration in main.py
        })
        # new_posts.append(submission.id)
        num_posts += 1
        print(f"Added post {submission.id} to {company}")

        sleep(10)  # Sleep to respect Reddit's API rate limits

    reddit_companies_posts[company]["from_date"] = datetime.now().strftime(date_format)

    # Save the updated data
    print("Saving file...")
    with open(reddit_companies_posts_path, 'w') as file:
        json.dump(reddit_companies_posts, file, indent=4)

    return new_posts, reddit_companies_posts

