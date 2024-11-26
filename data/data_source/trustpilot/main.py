from time import sleep
import json
from datetime import datetime
from trustpilot import scrape_and_send_reviews
from kafka_producer import KafkaProducer

if __name__ == "__main__":
    # this doesn't work yet because I can't connect to the kafka container, probably because need external port
    client_id = "trustpilot-producer"
    bootstrap_servers = "kafka:9092"
    source = "trustpilot"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    print(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}", flush=True)

    # Load companies and dates of the last scraping
    companies_from_date_path = "urls-trustpilot.json"
    with open(companies_from_date_path, 'r') as file:
        companies_date = json.load(file)
    print(f"Companies and dates of the last scraping loaded from {companies_from_date_path}", flush=True)
    for company in companies_date.keys():
        print(f"Company: {company}, Last scraping: {companies_date[company]}", flush=True)

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                    
    while True:
        for company in companies_date.keys():
            
            try:
                from_date = datetime.strptime(companies_date[company], date_format)
            except:
                raise AssertionError(f"The date '{companies_date[company]}' does NOT match the format '{date_format}'")
            
            scrape_and_send_reviews(company=company, 
                                    from_date = from_date,
                                    date_format = date_format,
                                    producer = producer,
                                    language="en")
            
            # Update the date of the last scraping
            companies_date[company] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(companies_date, file)

        sleep(30)  

        # Update dates of the last scraping
        with open(companies_from_date_path, 'r') as file:
            companies_date = json.load(file)
