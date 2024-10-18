from time import sleep
import json
from datetime import datetime
from kafka_producer import KafkaProducer
from dotenv import load_dotenv
import googleapiclient.discovery
import os

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
    companies_from_date_path = "urls-youtube.json"
    with open(companies_from_date_path, 'r') as file:
        companies_date = json.load(file)
    print(f"Companies and dates of the last scraping loaded from {companies_from_date_path}")
    for company in companies_date.keys():
        print(f"Company: {company}, Last scraping: {companies_date[company]}")

    date_format = "%Y-%m-%dT%H:%M:%SZ"

    while True:
        for company in companies_date.keys():
            
            try:
                from_date = datetime.strptime(companies_date[company], date_format)
            except:
                raise AssertionError(f"The date '{companies_date[company]}' does NOT match the format '{date_format}'")
            
            
            
            # Update the date of the last scraping
            companies_date[company] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(companies_date, file)

        sleep(30)  

        # Update dates of the last scraping
        with open(companies_from_date_path, 'r') as file:
            companies_date = json.load(file)