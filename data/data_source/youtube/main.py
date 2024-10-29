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
    print("Environment variables loaded")

    api_service_name = "youtube"
    api_version = "v3"
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")

    youtube_scraper = googleapiclient.discovery.build(
        api_service_name, 
        api_version, 
        developerKey=DEVELOPER_KEY
    )
    print("YouTube API service instantiated")

    # this doesn't work yet because I can't connect to the kafka container, probably because need external port
    client_id = "youtube-producer"
    bootstrap_servers = "kafka:9092"
    source = "youtube"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    print(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}")
    
    # Load companies and dates of the last scraping
    companies_videos_path = "youtube_companies_videos.json"
    with open(companies_videos_path, 'r') as file:
        companies_videos = json.load(file)
    print(f"Companies and videos of the last scraping loaded from {companies_videos_path}")
    for company in companies_videos.keys():
        print(f"Company: {company}, Last scraping: {companies_videos[company]}")

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    
    while True:
        
        for company in companies_videos.keys():
            
            print(f"Searching for new videos for {company}")
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
                    print(companies_videos[company]["videos"][video])

                    next_page_token = getcomments_video(video = video,
                                                        youtube_scraper = youtube_scraper,
                                                        company = company,
                                                        producer = producer,
                                                        max_num_comments = companies_videos[company]["max_num_comments_per_scraping"],
                                                        next_page_token = companies_videos[company]["videos"][video]["next_page_token"],
                                                        from_date = companies_videos[company]["videos"][video]["date_last_scrape"])
                    if not next_page_token:
                        # Update the date of the last scraping because all comments have been collected
                        print(f"All comments have been collected for video {video}. Updating the date of the last scraping.")
                        companies_videos[company]["videos"][video]["date_last_scrape"] = datetime.now().strftime(date_format)
                    companies_videos[company]["videos"][video]["next_page_token"] = next_page_token
                    with open(companies_videos_path, 'w') as file:
                        json.dump(companies_videos, file, indent=4)

        print("Sleeping for 5 minutes...")
        sleep(300)  
        print("Waking up!")
        with open(companies_videos_path, 'r') as file:
            companies_videos = json.load(file)
