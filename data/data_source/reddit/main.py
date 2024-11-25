import sys
import os

# go to parent dir to import kaka_producer 
# naive solution by now but Im not managing to import it in a better way :( will check for it
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
from kafka_producer import KafkaProducer
# back to reddit dir
sys.path.remove(parent_dir)
import json
from time import sleep
from datetime import datetime
from dotenv import load_dotenv
import praw
from reddit import getcomments_reddit, search_posts

if __name__ == "__main__":
    
    sleep(10)
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "reddit.env"))
    print("current dir: ", os.getcwd())
    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_username = os.getenv("REDDIT_USERNAME")
    reddit_user_agent = os.getenv("REDDIT_USER_AGENT")
    reddit_user_password = os.getenv("REDDIT_USER_PASSWORD")
    print(f"Reddit client ID: {reddit_client_id}")
    
    # Initialize Reddit Scraper
    try:
        reddit_scraper = praw.Reddit(
            client_id=reddit_client_id,
            client_secret=reddit_client_secret,
            user_agent=reddit_user_agent,
            username=reddit_username,  
            password=reddit_user_password
        )
        print(f"Logged in as: {reddit_scraper.user.me()}")
    except Exception as e:
        print(f"Error initializing Reddit scraper: {e}")
        exit(1)
    
    # Initialize Kafka Producer
    try:
        client_id = "reddit-producer"
        bootstrap_servers = "localhost:9092"  # Update as per your setup
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=client_id)
        print(f"Kafka producer '{client_id}' connected to '{bootstrap_servers}'")
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        exit(1)
    
    # Load companies and scraping info
    companies_path = "reddit_companies.json"
    try:
        with open(companies_path, 'r') as file:
            companies = json.load(file)
        print(f"Loaded Reddit submissions data from {companies_path}")
    except FileNotFoundError:
        companies = {}
        print(f"{companies_path} not found. Starting with empty Reddit submissions data.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {companies_path}: {e}")
        companies = {}
    
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    
    while True:
        for company in companies.keys():
            print(f"Searching for new submissions for {company}")
            try:
                new_submissions, companies_submissions = search_posts(
                    query=companies[company].get("query", ""),
                    after_date=companies[company].get("from_date", "2023-01-01T00:00:00Z"),
                    reddit_client=reddit_scraper,
                    company=company,
                    max_posts=companies[company].get("max_submissions", 100),
                    subreddit_list=companies[company].get("subreddits", [company])
                )
            except Exception as e:
                print(f"Error during search_posts for {company}: {e}")
                continue

            # Ensure submissions list exists
            if "submissions" not in companies_submissions.keys():
                companies_submissions["submissions"] = []

            # Update submissions with new ones
            companies_submissions["submissions"].extend(new_submissions)
            
            for submission in new_submissions:

                submission_id = submission.get("submission_id")
                from_date = submission.get("from_date")
                max_num_comments = submission.get("max_num_comments", 100)
                
                if not submission_id or not from_date:
                    print(f"Invalid submission data: {submission_id}. Skipping.")
                    continue

                after_comment_id = None

                print(f"Fetching comments for submission {submission_id} of company {company}")
                try:
                    after_comment_id = getcomments_reddit(
                        submission_id=submission_id,
                        reddit_client=reddit_scraper,
                        from_date=from_date,
                        company=company,
                        max_num_comments=max_num_comments,
                        producer=producer,
                        save_submission=True,
                        after_comment_id=after_comment_id
                    )
                except Exception as e:
                    print(f"Error fetching Reddit comments for submission {submission_id}: {e}")
                    continue

                # Update the submission's from_date to now
                submission["from_date"] = datetime.now().strftime(date_format)

            # Save the updated Reddit submissions data
            try:
                with open(companies_path, 'w') as file:
                    json.dump(companies, file, indent=4)
                print(f"Updated Reddit submissions data saved to {companies_path}")
            except Exception as e:
                print(f"Error saving Reddit submissions data: {e}")

        # Sleep before the next iteration
        print("Sleeping for 60 seconds...   zzzz....")
        sleep(60)
