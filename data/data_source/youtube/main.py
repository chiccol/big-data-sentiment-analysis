from kafka_producer import KafkaProducer
import googleapiclient.discovery
from youtube import search_videos, getcomments_video

import logging
from config import CONFIG
from time import sleep
from datetime import datetime, timedelta
import json
import os
from datetime import datetime
from dotenv import load_dotenv

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

def main() -> None:
    """
    Main function for scraping YouTube comments and managing data flow.
    It initializes the YouTube scraper, Kafka producer, and processes
    data for companies specified in the configuration file. It periodically fetches
    new videos, retrieves comments, and updates the company's data in JSON files.

    Workflow:
        1. Load YouTube API keys and initialize the scraper.
        2. Connect to a Kafka producer for streaming data.
        3. Load company information, including queries and scraping details.
        4. Search for new videos and filter them based on video criteria.
        5. Retrieve comments for relevant videos, publishing them to Kafka.
        6. Update the scraping details for each company in the JSON file.
        7. Handle quota limits, pausing when necessary, and restart scraping the next day.

    Configuration:
        - Parameters are stored in youtube_companies_videos.json and config.py.
        - The `youtube.env` file contains the YouTube API keys.

    Quota Management:
        - Automatically switches to backup API keys if the daily quota is exceeded.
        - Pauses scraping until the next day when all keys are exhausted.

    Sleep Intervals:
        - The scraper pauses for 10 minutes after each cycle unless quota limits are hit.
        - If quota is exhausted, the scraper calculates the time until the next day and sleeps accordingly.

    Returns:
        None. The function runs indefinitely, periodically updating the JSON file and sending comments to Kafka.
    """
    # Load developer key for YouTube API and instantiate the scraper
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
    DEVELOPER_KEY_2 = os.getenv("DEVELOPER_KEY_2")
    extra_keys = [DEVELOPER_KEY_2]
    
    youtube_scraper = googleapiclient.discovery.build(
        CONFIG['api_service_name'], 
        CONFIG['api_version'], 
        developerKey=DEVELOPER_KEY,
        static_discovery = False
    )
    
    # this doesn't work yet because I can't connect to the kafka container, probably because need external port
    producer = KafkaProducer(
        bootstrap_servers=CONFIG['bootstrap_servers'], 
        client_id = CONFIG['client_id']
        )
    logger.info(f"Kafka producer {CONFIG['client_id']} connected to {CONFIG['bootstrap_servers']} for Youtube") 
    
    # Load companies and dates of the last scraping
    with open(CONFIG['companies_videos_path'], 'r') as file:
        companies_videos = json.load(file)
    logger.info(f"Companies and videos of the last scraping loaded from {CONFIG['companies_videos_path']}")
    for company in companies_videos.keys():
        logger.info(f"Company: {company}, Last scraping: {companies_videos[company]}")
    
    while True:
        company_msg = dict()
        total_comments_scraped = 0

        for company in companies_videos.keys():
            company_msg[company] = 0
            logger.info(f"Searching for new videos for {company}")

            new_videos, companies_videos, youtube_scraper = search_videos(
                query = companies_videos[company]["query"],
                publishedAfter = companies_videos[company]["search_from_date"],
                youtube_scraper = youtube_scraper,
                extra_keys = extra_keys,
                company = company,
                max_videos = companies_videos[company]["max_videos"],
                relevanceLanguage = companies_videos[company]["relevance_language"],
                min_duration = companies_videos[company]["min_duration"],
                min_comment = companies_videos[company]["min_comment"],
                min_view = companies_videos[company]["min_view"]
                )
            logger.info(f"Found {len(new_videos)} new videos for {company}")

            for video in companies_videos[company]["videos"]:
                if companies_videos[company]["videos"][video] not in ["too_short", "currently_irrelevant"] and\
                    (companies_videos[company]["videos"][video] in new_videos or \
                        companies_videos[company]["videos"][video].get("next_page_token", None) != None):
                    info = companies_videos[company]["videos"][video]
                    logger.info(f"Currenty checking {info}")

                    next_page_token, num_comments, youtube_scraper = getcomments_video(
                        video = video,
                        youtube_scraper = youtube_scraper,
                        extra_keys = extra_keys,
                        company = company,
                        producer = producer,
                        max_num_comments = companies_videos[company]["max_num_comments_per_scraping"],
                        next_page_token = companies_videos[company]["videos"][video]["next_page_token"],
                        from_date = companies_videos[company]["videos"][video]["date_last_scrape"]
                        )
                    
                    logger.info(f"Collected {num_comments} comments from video {video}")
                    company_msg[company] += num_comments
                    total_comments_scraped += num_comments
                    
                    # Update scraping details
                    if not next_page_token:
                        # Update the date of the last scraping because all comments have been collected
                        logger.info(f"All comments have been collected for video {video}. Updating the date of the last scraping.")
                        companies_videos[company]["videos"][video]["date_last_scrape"] = datetime.now().strftime(CONFIG['date_format'])
                    companies_videos[company]["videos"][video]["next_page_token"] = next_page_token

                    with open(CONFIG['companies_videos_path'], 'w') as file:
                        json.dump(companies_videos, file, indent=4)
        
        logger.info(f"Total comments scraped: {total_comments_scraped}")
        logger.info("Company | Comments Count:")

        for company, count in company_msg.items():
            logger.info(f"{company}: {count}") 

        # Handle quota exhaustion
        if not youtube_scraper:
            logger.error(f"Quota exceeded for company {company}. Stopping the scraping.")
                
            # Calculate the time remaining until the next day
            now = datetime.now()
            next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time_until_next_day = (next_day - now).total_seconds()
            hours, remainder = divmod(time_until_next_day, 3600)
            minutes, seconds = divmod(remainder, 60)
            logger.info(
                f"Sleeping until the next day: {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds."
                )
            sleep(time_until_next_day)
            
            logger.info("Restarting the scraping.")
            extra_keys = [DEVELOPER_KEY_2]
            youtube_scraper = googleapiclient.discovery.build(
                CONFIG['api_service_name'], 
                CONFIG['api_version'], 
                developerKey=DEVELOPER_KEY
            )
        else:
            logger.info("Sleeping for 10 minutes")
            sleep(600)  

        # Reload company data to stay updated
        with open(CONFIG['companies_videos_path'], 'r') as file:
            companies_videos = json.load(file) 

if __name__ == "__main__":
    main()