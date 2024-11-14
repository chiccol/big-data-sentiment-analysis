from time import sleep
from mongodb_manager import MongoDB 
from kafka_consumer import KafkaConsumer

def main():
    print("Trying to use mongo and kafka")
    mongo = MongoDB()
    mongo.create_db('reviews')
    mongo_consumer = KafkaConsumer()
    # when sleeping this can trigger the sleep event in kafka, kicking the consumer out of the loop, thus a better strategy
    # may be to close the connection with kafka and reopen it after having slept
    mongo_consumer.consume_messages(consumer=mongo)
    print("Consumed messages, sleeping for 1.5 hours")
    sleep(5400)

if __name__ == "__main__":
    main()