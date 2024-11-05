from time import sleep
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka_producer import KafkaProducer
import os
import praw
from reddit import getcomments_reddit, search_submissions

if __name__ == "__main__":
    
    sleep(10)
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "reddit.env"))

    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_username = os.getenv("REDDIT_USERNAME")
    reddit_user_agent = os.getenv("REDDIT_USER_AGENT")
    
    reddit_scraper = praw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=reddit_user_agent,
        username=reddit_username
    )
    
    print(f"Logged in as: {reddit_scraper.user.me()}")
    
    client_id = "reddit-producer"
    bootstrap_servers = "kafka:9092"
    source = "reddit"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    
    print(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}")
    
    # Load companies and dates of the last scraping
    companies_submissions_path = "reddit_companies_submissions.json"
    with open(companies_submissions_path, 'r') as file:
        companies_submissions = json.load(file)
    print(f"Companies and submissions of the last scraping loaded from {companies_submissions_path}")
    for company in companies_submissions.keys():
        print(f"Company: {company}, Last scraping: {companies_submissions[company]}")
        
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    
    while True:
        
        for company in companies_submissions.keys():
            
            print(f"Searching for new submissions for {company}")
            new_submissions, companies_submissions = search_submissions(query = companies_submissions[company]["query"],
                                                         from_date = companies_submissions[company]["search_from_date"],
                                                         reddit_scraper = reddit_scraper,
                                                         company = company,
                                                         max_submissions = companies_submissions[company]["max_submissions"])
            
            for submission in companies_submissions[company]["submissions"]:
                submission_id = submission["submission_id"]
                from_date = submission["from_date"]
                max_num_comments = submission["max_num_comments"]
                
                after_comment_id = None
                
                print(f"Fetching comments for submission {submission_id} of company {company}")
                after_comment_id = getcomments_reddit(
                    submission_id,
                    reddit_scraper,
                    from_date,
                    company,
                    max_num_comments,
                    producer,
                    save_submission=True,
                    after_comment_id=after_comment_id
                )
                
                submission["from_date"] = datetime.now().strftime(date_format)
                
            companies_submissions[company]["submissions"] = companies_submissions[company]["submissions"] + new_submissions
            
            with open(companies_submissions_path, 'w') as file:
                json.dump(companies_submissions, file, indent=4)
            print(f"Companies and submissions updated and saved to {companies_submissions_path}")
    
        sleep(60)