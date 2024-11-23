from time import sleep
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka_producer import KafkaProducer
import googleapiclient.discovery
import os
from youtube import search_videos, getcomments_video

if __name__ == "__main__":
    # Load developer key for YouTube API and instantiate the scraper
    load_dotenv()
    print("Environment variables loaded", flush=True)

    api_service_name = "youtube"
    api_version = "v3"
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")

    youtube_scraper = googleapiclient.discovery.build(
        api_service_name, 
        api_version, 
        developerKey=DEVELOPER_KEY
    )
    print("YouTube API service instantiated", flush=True)

    # this doesn't work yet because I can't connect to the kafka container, probably because need external port
    client_id = "youtube-producer"
    bootstrap_servers = "kafka:9092"
    source = "youtube"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    print(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}", flush=True)
    
    # Load companies and dates of the last scraping
    companies_videos_path = "youtube_companies_videos.json"
    with open(companies_videos_path, 'r') as file:
        companies_videos = json.load(file)
    print(f"Companies and videos of the last scraping loaded from {companies_videos_path}", flush=True)
    for company in companies_videos.keys():
        print(f"Company: {company}, Last scraping: {companies_videos[company]}", flush=True)

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    
    while True:
        company_msg = dict()
        total_comments_scraped = 0
        for company in companies_videos.keys():
            company_msg[company] = 0
            print(f"Searching for new videos for {company}", flush=True)
            new_videos, companies_videos = search_videos(query = companies_videos[company]["query"],
                                                         publishedAfter = companies_videos[company]["search_from_date"],
                                                         youtube_scraper = youtube_scraper,
                                                         company = company,
                                                         max_videos = companies_videos[company]["max_videos"],
                                                         relevanceLanguage = companies_videos[company]["relevance_language"],
                                                         min_duration = companies_videos[company]["min_duration"],
                                                         min_comment = companies_videos[company]["min_comment"],
                                                         min_view = companies_videos[company]["min_view"])
            
            for video in companies_videos[company]["videos"]:
                if companies_videos[company]["videos"][video] not in ["too_short", "currently_irrelevant"] and\
                    (companies_videos[company]["videos"][video] in new_videos or \
                        companies_videos[company]["videos"][video].get("next_page_token", None) != None):
                    print(companies_videos[company]["videos"][video], flush=True)

                    next_page_token, num_comments = getcomments_video(video = video,
                                                                      youtube_scraper = youtube_scraper,
                                                                      company = company,
                                                                      producer = producer,
                                                                      max_num_comments = companies_videos[company]["max_num_comments_per_scraping"],
                                                                      next_page_token = companies_videos[company]["videos"][video]["next_page_token"],
                                                                      from_date = companies_videos[company]["videos"][video]["date_last_scrape"])
                    print(f"Collected {num_comments} comments from video {video}", flush=True)
                    company_msg[company] += num_comments
                    total_comments_scraped += num_comments
                    if not next_page_token:
                        # Update the date of the last scraping because all comments have been collected
                        print(f"All comments have been collected for video {video}. Updating the date of the last scraping.", flush=True)
                        companies_videos[company]["videos"][video]["date_last_scrape"] = datetime.now().strftime(date_format)
                    companies_videos[company]["videos"][video]["next_page_token"] = next_page_token
                    with open(companies_videos_path, 'w') as file:
                        json.dump(companies_videos, file, indent=4)

        print(f"Total comments scraped: {total_comments_scraped}", flush=True)
        print("Scraped report", flush=True)
        print("Company | Comments Count")
        for company in company_msg.keys():
            print(f"{company}: {company_msg[company]}")
        print("Sleeping for 5 minutes...", flush=True)
        sleep(10)  
        print("Waking up!", flush=True)
        with open(companies_videos_path, 'r') as file:
            companies_videos = json.load(file)
