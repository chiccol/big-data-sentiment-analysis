import time
from mongo_manager import MongoManager 
from kafka_consumer import KafkaConsumer
import json

def write_on_mongo(msg, mongo):
    try:
        topic = str(msg.topic())
        if topic not in mongo.collection_list:
            # create_collection already switches to that collection
            mongo.create_collection(topic)
        # if the collection is not new, we switch to it to write correctly
        mongo.change_collection(topic)
        try:
            # Converts the string output of msg.value().decode('utf-8')) into a list of dict
            mongo.insert_list_dict(json.loads(msg.value().decode('utf-8')))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {str(e)}")
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            
    except Exception as e:
        print(f"Error writing to MongoDB: {str(e)}")

def consume_messages(kafka, mongo, check_interval = 30):
    print("Entered consume message once function")
    metadata = kafka.get_metadata(timeout=10)
    topics = list(metadata.topics.keys())
    
    if not topics:
            print("No topics found in Kafka broker.")
            return
    
    print("Topics available in broker:", topics)

    for topic in topics:
        if topic == '__consumer_offsets':  # Ignore internal Kafka topic
            continue
        
        print(f"\nRetrieving messages from {topic}")
        partitions = metadata.topics[topic].partitions.keys()
        print(f"Partitions for {topic}: {partitions}")
        for partition in partitions:
            count = 0
            print(f"Reading all messages from topic '{topic}', partition {partition}")
            while True:
                msg = kafka.poll_message(timeout=1.0)

                if msg is None:
                    print("No more messages available.")
                    break
                elif msg.error():
                    print(f"Error while polling message: {msg.error()}")
                else:
                    count += 1
                    print(f"Message {count + 1} from partition {partition}: {msg.value().decode('utf-8')}")
                    print(f"Attempting to write it on mongo on collection {topic}")
                    write_on_mongo(msg, mongo)

def inspect_broker(self, num_messages: int = 5):
    """
    Inspect the broker to see what topics, partitions, and messages are available.
    Attempts to consume a few messages from each topic partition.
    """
    print("\nInspecting Kafka broker...\n")
    
    # Step 1: List all topics
    metadata = self.consumer.list_topics(timeout=10)
    topics = metadata.topics.keys()
    
    if not topics:
        print("No topics found in Kafka broker.")
        return
    
    print("Topics available in broker:", topics)
    
    # Step 2: Inspect each topic's partitions and messages
    for topic in topics:
        if topic == '__consumer_offsets':  # Ignore internal Kafka topic
            continue
        
        print(f"\nInspecting topic: {topic}")
        partitions = metadata.topics[topic].partitions.keys()
        print(f"Partitions for {topic}: {partitions}")
        
        # Step 3: Try to consume a few messages from each partition
        for partition in partitions:
            print(f"Reading up to {num_messages} messages from topic '{topic}', partition {partition}")
            count = 0
            while count < num_messages:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    print("No more messages available.")
                    break
                elif msg.error():
                    print(f"Error while polling message: {msg.error()}")
                else:
                    print(f"Message {count + 1} from partition {partition}: {msg.value().decode('utf-8')}")
                    count += 1
            if count == 0:
                print(f"No messages found in partition {partition}.")


def consume_loop(kafka, mongo, check_interval=30):
    print("Entered consume loop function")
    # Subscribe to initial topics
    kafka.get_topics()
    last_check = time.time()

    while True:
        # Check for new topics periodically
        current_time = time.time()
        if current_time - last_check >= check_interval:
            kafka.get_topics()  # Update subscription if new topics are found
            last_check = current_time
        
        # Poll Kafka for messages
        msg = kafka.poll(timeout=1.0)
        if msg is None:
            continue  # No message available; continue polling
        if msg.error():
            print(f"Error polling message: {msg.error()}")
        else:
            print("Attempting to write on Mongo")
            write_on_mongo(msg, mongo)

def main():
    print("Trying to use mongo and kafka")
    mongo = MongoManager()
    mongo.create_db('reviews')
    kafka = KafkaConsumer()
    consume_messages(kafka, mongo)
    print("Messages have been consumed")

if __name__ == "__main__":
    main()
