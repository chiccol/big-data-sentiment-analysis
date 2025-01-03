from time import sleep
import json
from datetime import datetime
from trustpilot import scrape_and_send_reviews
from kafka_producer import KafkaProducer
import logging
from config import CONFIG

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

def main():
    """
    Main function to initialize the Kafka producer, load company scraping metadata, 
    and orchestrate the scraping and sending of Trustpilot reviews in a continuous loop.
    Steps:
        1. Initialize the Kafka producer.
        2. Load the JSON file (CONFIG["companies_date_path"]) containing the companies and their last scraping dates.
        3. Scrape reviews for each company in the JSON.
        4. Serialize and send reviews to Kafka.
        5. Update the last scraping date and save it to the JSON file.
        6. Repeat the process in a continuous loop with a pause to avoid being blocked by Trustpilot.
    Notes:
        - The name of the companies and the start date for scraping is defined by urls-trustpilot.json.
        - The producer configuration and other parameters are defined in the confi.py.
        - Dates are expected to follow the format defined in `CONFIG["date_format"]`.
    """
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers = CONFIG["bootstrap_servers"], 
        client_id = CONFIG["client_id"]
        )
    logger.info(f"Kafka producer {CONFIG['client_id']} connected to {CONFIG['bootstrap_servers']} for trustpilot")
    
    # Load companies and dates of the last scraping
    companies_from_date_path = "companies.json"
    with open(companies_from_date_path, 'r') as file:
        companies = json.load(file)
    logger.info(f"Companies and dates of the last scraping loaded from {companies_from_date_path}") 
    for company in companies.keys():
        logger.info(f"Company: {company}, Last scraping: {companies[company]['last_scraping']}")

    # Sleeping time in hours, minutes, and seconds
    hours = CONFIG['sleep_time'] // 3600
    minutes = (CONFIG['sleep_time'] % 3600) // 60
    remaining_seconds = CONFIG['sleep_time'] % 60
                    
    while True:
        for company in companies.keys():
            
            try:
                from_date = datetime.strptime(companies[company]["last_scraping"], date_format)
            except:
                raise AssertionError(f"The date '{companies[company]['last_scraping']}' does NOT match the format '{date_format}'")
            
            scrape_and_send_reviews(company=companies[company]["website"], 
                                    from_date = from_date,
                                    date_format = CONFIG["date_format"],
                                    producer = producer,
                                    language="en")
            
            # Update the date of the last scraping
            companies[company]["last_scraping"] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(companies, file)

        # Sleep to avoid being blocked by Trustpilot
        logger.info(f"Sleeping for {hours} hours, {minutes} minutes, and {remaining_seconds} seconds")
        sleep(CONFIG["sleep_time"])  

        # Update dates of the last scraping
        with open(companies_from_date_path, 'r') as file:
            companies = json.load(file)
