from time import sleep
from kafka_producer import KafkaProducer
import logging
from pseudo import data_gen

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("pseudo-producer")
logger.info("Started logging")

if __name__ == "__main__":
    client_id = "pseudo-producer"
    bootstrap_servers = "kafka:9092"
    source = "pseudo"
    logger.info(f"Pseudo producer is up")
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id = client_id)
    logger.info(f"Kafka producer {client_id} connected to {bootstrap_servers} for {source}")

    # List of companies (expand as needed)
    companies = [
        'Apple', 'Google', 'Microsoft', 'Amazon', 'Facebook', 
        'Tesla', 'Netflix', 'Spotify', 'Uber', 'Airbnb'
    ]
    companies = ['apple', 'google']
    for company in companies:
        logger.info(f"Sending fake data for {company}")
        data = data_gen(company = company, producer = producer, num_entries= 500)
        logger.info(f"Sent fake data for {company}")
