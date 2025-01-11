import random
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import pandas as pd
from faker import Faker
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
logger = logging.getLogger("pseudo-producer")
logger.info("Started logging")
def encode_message_to_parquet(data: list[dict]) -> bytes: 
    """
    Encodes a list of dictionaries into a in-memory parquet table.

    Args:
        data -> list of dicts

    Returns:
        bytes
    """
    # Infer the schema from the data
    schema = pa.Table.from_pandas(pd.DataFrame(data)).schema

    # Convert the data to an Arrow Table using the inferred schema
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    # Write the table to an in-memory bytes buffer as Parquet
    buffer = BytesIO()
    pq.write_table(table, buffer)
    logger.info(f"Encoded the message successfully")
    # Return the Parquet bytes for saving or sending
    return buffer.getvalue()

def data_gen(company: str, producer, num_entries: int = 100) -> None:
    """
    Generates fake data matching our schema.
    Following Trustpilots' architecture, it takes the producer in input and delivers the message as well.
    Kafka will publish these messages in the respective topic named company (given in input).
    It emulates Youtube and Trustpilot.
    
    Args:
        company (str): Company name, it is a string used to choose to which topic delives the fakely produced messages.
        num_entries (int): Number of fake entries to generate. Defaults to 100.
        producer: An instance of a KafkaProducer class. 
    Returns:
        None
    """
    fake = Faker()
    
    # List of potential sources
    sources = ['Trustpilot', 'youtube', 'reddit']
    reviews = []
    logger.info(f"Generating {num_entries} datapoints for {company}")
    for _ in range(num_entries):

        source = random.choice(sources)
        
        # Generate data based on source
        # It uses the faker library to emulate dates, and text. Random is used for integer generations.
        if source == 'youtube':
            entry = {
                'source': source,
                'date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                'company': company,
                'text': fake.paragraph(),
                'videoid': fake.uuid4(),
                'like_count': random.randint(1, 5),
                'youtube_reply_count': random.randint(1, 10000)
            }
        elif source == "Trustpilot":
            entry = {
                'source': source,
                'date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                'company': company,
                'text': fake.paragraph(),
                'stars': random.randint(1, 5),  # No need for round
                'location': fake.city()}
        else:
            entry = {
                'source': source,
                'date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                'company': company,
                'text': fake.paragraph(),
                'subreddit': "random",
                'vote': random.randint(1, 100),
                'reddit_reply_count': random.randint(1, 1000)  # No need for round
                }

        reviews.append(entry)
    
    result_encoded = encode_message_to_parquet(reviews)
    producer.produce(record = result_encoded, topic = company)
    logger.info(f"Sent a pseudo encoded review of length {len(reviews)} for company {company}")
    return None
