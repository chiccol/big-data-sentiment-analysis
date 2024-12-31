from time import sleep
import json
from datetime import datetime
from trustpilot import scrape_and_send_reviews
from kafka_producer import KafkaProducer
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
logger = logging.getLogger("trustpilot-producer")
logger.info("Started logging")

if __name__ == "__main__":
    client_id = "trustpilot-producer"
    bootstrap_servers = "kafka:9092"
    source = "trustpilot"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    logger.info(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}")
    # Load companies and dates of the last scraping
    companies_from_date_path = "companies.json"
    with open(companies_from_date_path, 'r') as file:
        companies = json.load(file)
    logger.info(f"Companies and dates of the last scraping loaded from {companies_from_date_path}") 
    for company in companies.keys():
        logger.info(f"Company: {company}, Last scraping: {companies[company]['last_scraping']}")

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                    
    while True:
        for company in companies.keys():
            
            try:
                from_date = datetime.strptime(companies[company]["last_scraping"], date_format)
            except:
                raise AssertionError(f"The date '{companies[company]['last_scraping']}' does NOT match the format '{date_format}'")
            
            scrape_and_send_reviews(company=companies[company]["website"], 
                                    from_date = from_date,
                                    date_format = date_format,
                                    producer = producer,
                                    language="en")
            
            # Update the date of the last scraping
            companies[company]["last_scraping"] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(companies, file)

        sleep(30)  

        # Update dates of the last scraping
        with open(companies_from_date_path, 'r') as file:
            companies = json.load(file)
