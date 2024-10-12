from time import sleep
import json
from datetime import datetime
from trustpilot import scrape_reviews
from data_source.kafka_producer import KafkaProducer

if __name__ == "__main__":
    
    producer = KafkaProducer(bootstrap_servers="kafka", client_id = "trustpilot-producer")

    # Load companies and dates of the last scraping
    companies_from_date_path = "urls-trustpilot.json"
    with open(companies_from_date_path, 'r') as file:
        companies_date = json.load(file)

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                    
    while True:
        for company in companies_date.keys():
            
            try:
                from_date = datetime.strptime(companies_date[company], date_format)
            except:
                raise AssertionError(f"The date '{companies_date[company]}' does NOT match the format '{date_format}'")
            
            scrape_reviews(company=company, 
                           from_date = from_date,
                           date_format = date_format,
                           language="en")
            
            # Update the date of the last scraping
            companies_date[company] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(companies_date, file)

        sleep(30)  

        # Update dates of the last scraping
        with open(companies_from_date_path, 'r') as file:
            companies_date = json.load(file)
