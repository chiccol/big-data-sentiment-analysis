import os
import praw
from dotenv import load_dotenv
from datetime import datetime
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
    submission = reddit_client.submission(id=submission_id)

    if save_submission:
        # Fetch submission data
        # submission_data = {
        #     "type": "submission",
        #     "source": "reddit",
        #     "submissionId": submission_id,
        #     "submissionTitle": submission.title,
        #     "submissionText": submission.selftext,
        #     "createdAt": datetime.fromtimestamp(submission.created_utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        #     "score": submission.score,
        #     "numComments": submission.num_comments,
        #     "url": submission.url,
        #     "author": str(submission.author),
        #     "subreddit": str(submission.subreddit)
        # }
        submission_data = {
            "id": submission_id,
            "source": "reddit",
            "text": submission.title + ": " + submission.selftext,
            "date": datetime.fromtimestamp(submission.created_utc),
            "re-vote": submission.score,
            "re-reply-count": submission.num_comments
        }

        # Send submission data to Kafka
        submission_json = json.dumps(submission_data).encode('utf-8')
        producer.produce(record=submission_json, topic=company)
        print(f"Sent submission data for submission {submission_id} of company {company}")
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
            "date": comment_datetime,
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
        producer.produce(record=comments_json, topic=company)
        print(f"Sent {num_comments} comments for submission {submission_id} of company {company}")
    else:
        print(f"No new comments to fetch for submission {submission_id} of company {company}")
        after_comment_id = None

    sleep(5)  # Pause to respect API rate limits
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
    Search for Reddit posts matching the query and save relevant posts to a JSON file.

    Parameters:
    - query: str, the search query
    - after_date: str, the date in the format "YYYY-MM-DDTHH:MM:SSZ"
    - reddit_client: praw.Reddit, an authenticated Reddit client instance
    - company: str, the company name used in the JSON file
    - max_posts: int, the maximum number of posts to retrieve
    - min_comments: int, the minimum number of comments a post must have
    - min_score: int, the minimum score (upvotes - downvotes) a post must have
    - subreddit_list: list of str, subreddits to search in (defaults to all)
    """
    num_posts = 0
    reddit_companies_posts_path = "reddit_companies_posts.json"
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    after_timestamp = int(datetime.strptime(after_date, date_format).timestamp())
    new_posts = []

    # Load existing posts data
    try:
        with open(reddit_companies_posts_path, 'r') as file:
            reddit_companies_posts = json.load(file)
    except FileNotFoundError:
        reddit_companies_posts = {}

    if company not in reddit_companies_posts:
        reddit_companies_posts[company] = {
            "posts": {},
            "search_from_date": after_date
        }

    # Create a search query
    search_query = f"{query} timestamp:{after_timestamp}.."

    # Define subreddits to search in
    subreddits = '+'.join(subreddit_list) if subreddit_list else 'all'

    # Use Reddit's API to search for posts
    search_results = reddit_client.subreddit(subreddits).search(
        query=search_query,
        sort='new',
        syntax='cloudsearch',
        limit=None  # Fetch as many as possible, we'll handle the max_posts limit
    )

    for submission in search_results:
        if num_posts >= max_posts:
            print("Reached the maximum number of posts.")
            break

        submission_created = datetime.utcfromtimestamp(submission.created_utc).strftime(date_format)

        # Skip if the post is before the after_date
        if datetime.strptime(submission_created, date_format) < datetime.strptime(after_date, date_format):
            print(f"Post {submission.id} is older than {after_date}")
            continue

        # Check if the post meets the minimum criteria
        if submission.num_comments < min_comments:
            print(f"Post {submission.id} does not have enough comments.")
            continue

        if submission.score < min_score:
            print(f"Post {submission.id} does not have enough score.")
            continue

        # Check if the post is already in the data
        if submission.id in reddit_companies_posts[company]["posts"]:
            print(f"Post {submission.id} is already processed.")
            continue

        # Add the post to the data
        reddit_companies_posts[company]["posts"][submission.id] = {
            "date_last_scrape": after_date,
            "subreddit": str(submission.subreddit),
            "title": submission.title,
            "url": submission.url,
            "createdAt": submission_created,
            "author": str(submission.author),
            "numComments": submission.num_comments,
            "score": submission.score,
            "selftext": submission.selftext
        }
        new_posts.append(submission.id)
        num_posts += 1
        print(f"Added post {submission.id} to {company}")

        sleep(1)  # Sleep to respect Reddit's API rate limits

    reddit_companies_posts[company]["search_from_date"] = datetime.now().strftime(date_format)

    # Save the updated data
    print("Saving file...")
    with open(reddit_companies_posts_path, 'w') as file:
        json.dump(reddit_companies_posts, file, indent=4)

    return new_posts, reddit_companies_posts




if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Set up the Reddit client
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
        username=os.getenv("REDDIT_USERNAME")
    )
    print(f"Logged in as: {reddit.user.me()}")
    
    client_id = "reddit-producer"
    bootstrap_servers = "kafka:9092"
    source = "reddit"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    print(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}")
    

    # Define the parameters
    submission_id = "l7v4n9"
    from_date = "2022-01-01T00:00:00Z"
    company = "Tesla"
    max_num_comments = 10
    after_comment_id = None

    # Fetch comments from the Reddit submission
    after_comment_id = getcomments_reddit(
        submission_id,
        reddit,
        from_date,
        company,
        max_num_comments,
        producer,
        save_submission=True,
        after_comment_id=after_comment_id
    )
