from time import sleep
from mongodb_manager import MongoDB 
from kafka_consumer import KafkaConsumer
import json
import pyarrow.parquet as pq
from io import BytesIO

def decode_parquet(msg):
    # Use BytesIO to read the binary Parquet data
    buffer = BytesIO(msg)
    
    # Read the Parquet data back into an Arrow table
    table = pq.read_table(buffer)
    
    # Convert the Arrow table to a pandas DataFrame for easier manipulation
    decoded_msg = table.to_pylist()
    print(f"Function decode_parquet worked, this is the output: {decoded_msg}")
    return decoded_msg


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
            mongo.insert_list_dict(decode_parquet(msg.value()))
        # aggiusta gli errori con parquet
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {str(e)}")
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            
    except Exception as e:
        print(f"Error writing to MongoDB: {str(e)}")

def consume_messages(kafka, mongo):
    print("Entered consume message function")
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
        print(partitions)
        print(f"Partitions for {topic}: {partitions}")
        for partition in partitions:
            count = 0
            print(f"Reading all messages from topic '{topic}', partition {partition}")
            
            # Use a while loop to continuously poll for messages
            while True:
                msg = kafka.poll_message(timeout=1.0)
                
                if msg is None:
                    print("No more messages available.")
                    break
                elif msg.error():
                    print(f"Error while polling message: {msg.error()}")
                else:
                    count += 1
                    print(f"The message is of type {type(msg.value())} and it is {msg.value()}")
                    print(f"Attempting to write it on mongo on collection {topic}")
                    write_on_mongo(msg, mongo)

def main():
    print("Trying to use mongo and kafka")
    mongo = MongoDB()
    mongo.create_db('reviews')
    kafka = KafkaConsumer()
    # when sleeping this can trigger the sleep event in kafka, kicking the consumer out of the loop, thus a better strategy
    # may be to close the connection with kafka and reopen it after having slept
    consume_messages(kafka, mongo)
    print("Consumed messages, sleeping for 1.5 hours")
    sleep(5400)

if __name__ == "__main__":
    main()
