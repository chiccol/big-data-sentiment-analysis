from pyspark.sql import SparkSession
from kafka_consumer import KafkaConsumer
from utils import get_sentiment_udf, process_data, write_mongo, write_postgres 
import os
import time
import random
import logging
from time import sleep


def main():
    # Setup logger
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Output to console
            # Optionally add file logging
            # logging.FileHandler('app.log')
        ]
    )
    logger = logging.getLogger("spark-master")
    logger.info("Started logging")

    # Get venv variables 
    spark_master = os.getenv("SPARK_MASTER_HOST")
    spark_port = os.getenv("SPARK_MASTER_PORT")
    kafka_adv_external_listener = os.getenv("KAFKA_ADVERTISED_LISTENERS")
    client_id = os.getenv("CLIENT_ID")
    group_id = os.getenv("GROUP_ID")

    spark = SparkSession.builder \
        .master(f"spark://{spark_master}:{spark_port}") \
        .config("spark.mongodb.output.uri", f"mongodb://mongo:27017/") \
        .appName("Writer-Sentiment-Analysis") \
        .getOrCreate()

    max_retries = 5
    base_delay = 1.5  # seconds

    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_adv_external_listener, 
                client_id=client_id, 
                group_id=group_id
            )
            break
        except Exception as e:
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                delay = (base_delay ** attempt) + random.uniform(0, 1)
                logger.warning(f"Error initializing Kafka consumer. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Max retries reached. Exiting. {e}")
                raise
            # aggiungere cosa raisiamo :)
    logger.info("Initializing Kafka consumer...") 

    while True:
        logger.info("Getting data from Kafka...")
        all_messages, topics = consumer.consume_messages_spark()
        logger.info(f"We obtained {topics}")
        logger.info(f"Messages consumed with Spark: {len(all_messages)}")
        if all_messages:
            df = process_data(all_messages, spark)
            df_postgres = df.select(["source", "date", "company", "sentiment", "negative_probability", 
                                     "neutral_probability", "positive_probability", "tp_stars", "tp_location", 
                                     "yt_videoid", "yt_like_count", "yt_reply_count", "re_id", "re_subreddit",
                                     "re_vote", "re_reply_count"])
            write_postgres(df_postgres)
            df_mongo = df.select(["source", "date", "text", "company", "sentiment"])
            write_mongo(df_mongo, topics)
        else:
            logger.info(f"No data was consumed")
            logger.info(f"Sleeping for 15 seconds...")
            sleep(15)
if __name__ == "__main__":
    main()
