from time import sleep
from mongodb_manager import MongoDB, WordCountDB
from kafka_consumer import KafkaConsumer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting main script.")
    try:
        mongo = MongoDB()
        mongo.create_db('reviews')
        
        mongo_wc = WordCountDB()
        mongo_wc.create_db('word_count')
        logging.info("Databases created: 'reviews' and 'word_count'")
        
        mongo_consumer = KafkaConsumer()
        mongo_consumer.consume_messages_mongo(mongo_manager=mongo)
        
        # when sleeping this can trigger the sleep event in kafka, kicking the consumer out of the loop, thus a better strategy
        # may be to close the connection with kafka and reopen it after having slept
        logging.info("Consumed messages from Kafka. Sleeping for 1.5 hours.")
        sleep(5400)
    except Exception as e:
        logging.error(f"Error in main script: {e}")

if __name__ == "__main__":
    main()
