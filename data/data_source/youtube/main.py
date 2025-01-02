from time import sleep
from datetime import datetime, timedelta
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka_producer import KafkaProducer
import googleapiclient.discovery
import os
from youtube import search_videos, getcomments_video
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("youtube-producer")
logger.info("Started logging")

if __name__ == "__main__":
    # Load developer key for YouTube API and instantiate the scraper
    load_dotenv()

    api_service_name = "youtube"
    api_version = "v3"
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
    DEVELOPER_KEY_2 = os.getenv("DEVELOPER_KEY_2")
    extra_keys = [DEVELOPER_KEY_2]

    print(DEVELOPER_KEY_2, DEVELOPER_KEY, flush=True)

    youtube_scraper = googleapiclient.discovery.build(
        api_service_name, 
        api_version, 
        developerKey=DEVELOPER_KEY,
        static_discovery = False
    )
    
    # this doesn't work yet because I can't connect to the kafka container, probably because need external port
    client_id = "youtube-producer"
    bootstrap_servers = "kafka:9092"
    source = "youtube"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    logger.info(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}") 
    # Load companies and dates of the last scraping
    companies_videos_path = "youtube_companies_videos.json"
    with open(companies_videos_path, 'r') as file:
        companies_videos = json.load(file)
    logger.info(f"Companies and videos of the last scraping loaded from {companies_videos_path}")
    for company in companies_videos.keys():
        logger.info(f"Company: {company}, Last scraping: {companies_videos[company]}")
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    
    while True:
        company_msg = dict()
        total_comments_scraped = 0
        for company in companies_videos.keys():
            company_msg[company] = 0
            logger.info(f"Searching for new videos for {company}")
            new_videos, companies_videos, youtube_scraper = search_videos(query = companies_videos[company]["query"],
                                                                          publishedAfter = companies_videos[company]["search_from_date"],
                                                                          youtube_scraper = youtube_scraper,
                                                                          extra_keys = extra_keys,
                                                                          company = company,
                                                                          max_videos = companies_videos[company]["max_videos"],
                                                                          relevanceLanguage = companies_videos[company]["relevance_language"],
                                                                          min_duration = companies_videos[company]["min_duration"],
                                                                          min_comment = companies_videos[company]["min_comment"],
                                                                          min_view = companies_videos[company]["min_view"])
            logger.info(f"Found {len(new_videos)} new videos for {company}")
            for video in companies_videos[company]["videos"]:
                if companies_videos[company]["videos"][video] not in ["too_short", "currently_irrelevant"] and\
                    (companies_videos[company]["videos"][video] in new_videos or \
                        companies_videos[company]["videos"][video].get("next_page_token", None) != None):
                    info = companies_videos[company]["videos"][video]
                    logger.info(f"Currenty checking {info}")
                    next_page_token, num_comments, youtube_scraper = getcomments_video(video = video,
                                                                                       youtube_scraper = youtube_scraper,
                                                                                       extra_keys = extra_keys,
                                                                                       company = company,
                                                                                       producer = producer,
                                                                                       max_num_comments = companies_videos[company]["max_num_comments_per_scraping"],
                                                                                       next_page_token = companies_videos[company]["videos"][video]["next_page_token"],
                                                                                       from_date = companies_videos[company]["videos"][video]["date_last_scrape"])
                    logger.info(f"Collected {num_comments} comments from video {video}")
                    company_msg[company] += num_comments
                    total_comments_scraped += num_comments
                    if not next_page_token:
                        # Update the date of the last scraping because all comments have been collected
                        logger.info(f"All comments have been collected for video {video}. Updating the date of the last scraping.")
                        companies_videos[company]["videos"][video]["date_last_scrape"] = datetime.now().strftime(date_format)
                    companies_videos[company]["videos"][video]["next_page_token"] = next_page_token
                    with open(companies_videos_path, 'w') as file:
                        json.dump(companies_videos, file, indent=4)
        
        logger.info(f"Total comments scraped: {total_comments_scraped}")
        logger.info("Company | Comments Count:")

        for company, count in company_msg.items():
            logger.info(f"{company}: {count}") 

        if not youtube_scraper:
            logger.error(f"Quota exceeded for company {company}. Stopping the scraping.")
                
            # Calculate the time remaining until the next day
            now = datetime.now()
            next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time_until_next_day = (next_day - now).total_seconds()

            # Convert seconds into hours, minutes, and seconds
            hours, remainder = divmod(time_until_next_day, 3600)
            minutes, seconds = divmod(remainder, 60)
            logger.info(
                f"Sleeping until the next day: {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds."
                )
            sleep(time_until_next_day)
            
            logger.info("Restarting the scraping.")
            extra_keys = [DEVELOPER_KEY_2]
            youtube_scraper = googleapiclient.discovery.build(
                api_service_name, 
                api_version, 
                developerKey=DEVELOPER_KEY
            )
        else:
            logger.info("Sleeping for 10 minutes")
            sleep(600)  

        with open(companies_videos_path, 'r') as file:
            companies_videos = json.load(file)
