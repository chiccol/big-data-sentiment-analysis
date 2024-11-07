from time import sleep
from pymongo import MongoClient
from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime

def get_mongo_connection():
    """
    Establishes a MongoDB connection

    args:
        None

    returns:
        MongoClient object
    """
    try:
        # Create a MongoDB client
        client = MongoClient('mongodb://mongo:27017/')
        
        # Check the server status
        server_info = client.server_info()  # Will raise an exception if the connection is unsuccessful
        print("Connection to MongoDB was successful!")
        print("Server Info:", server_info)
        
        return client

    except ConnectionError:
        print("Failed to connect to MongoDB. Please check if the MongoDB service is running.")
    except Exception as e:
        print(f"An error occurred: {e}")

def mongodb_setup():
    """
    Creates a MongoDB client, database and collection.

    Args:
        None

    Returns:
        MongoDB collection
    """
    # 1. Connect to MongoDB
    # 2. Create a db called "mongoconsumer"
    # 3. Create a collection inside the db "mongoconsumer"
    # Connect to MongoDB running in Docker
    client = get_mongo_connection()    
    # Create or connect to a database
    db = client.mongoconsumer  # mongoconsumer is the name of the db
    # Create a collection (table equivalent in MongoDB).
    mongodb_collection = db.messages
    return mongodb_collection

def consumer_setup():
    """
    Sets a confluent-kafka.Consumer up and subscribes to a hard coded list of topics, it will be later improved. 

    args:
        None

    returns:
        confluent-kafka.Consumer object
    """
    for attempt in range(6):
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'mongogroup',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'security.protocol': 'PLAINTEXT'
        })
        
        topics = ["apple", "nvidia"]
        try:
            consumer.subscribe(topics)
            
            # Add a gentle connection check
            msg = consumer.poll(timeout=5.0)  # Poll with 5 second timeout
            if msg is None:
                print(f"Attempt {attempt + 1}: Connected to Kafka but no messages available yet...")
            else:
                print(f"Attempt {attempt + 1}: Successfully connected and received message!")
                return consumer
                
        except KafkaError as e:
            print(f"Attempt {attempt + 1}: Connection attempt failed: {e}")
            consumer.close()
            if attempt < 5:  # Don't sleep on the last attempt
                print("Waiting 5 seconds before retry...")
                sleep(5)
            continue

    print("Failed to establish connection after all attempts")
    return None    



def basic_consume_loop(yt_consumer):
    """
    Starts the consume endless loop. Sets the MongoDB collection up and the Consumer starts polling the broker.
    Each message gets checked and depending on the content it is either ignored, reported as an error (more resilient error 
    handling will be implemented eventually) or processed calling the msg_process function.

    args:
        yt_consumer: confluent-kafka.Consumer object

    returns:
        None
    """
    mongodb_collection = mongodb_setup()
    state = True
    # dict_test = {"key" : 9}
    # mongodb_collection.insert_one(dict_test)
    while state:
        msg = yt_consumer.poll(timeout=1.0)
        
        if msg is None:
            continue 
        if msg.error():
                print(f"Error: {msg.error()}")
        else:
            msg_process(msg, mongodb_collection)



def msg_process(msg, mongodb_collection):
    """
    Processes a single message obtained from the broker. 

    Assumes that the message is a binary encoded json. It decodes it from binary to json and from json to dict.
    We edit publishedAt field into a friendlier format for MongoDB, we may improve it later.
    MongoDB wants a dictionary in order to use insert_one.
    
    Args:
        msg (bytes): A binary-encoded message containing a JSON string.
        mongodb_collection: A MongoDB collection.

    Returns:
        None
    """

    # Decode the message value (assuming it is in JSON format)
    message_value = msg.value().decode('utf-8')

    # Convert JSON string to a Python dictionary
    data = json.loads(message_value)

    # Check and transform the publishedAt field if it exists
    if 'publishedAt' in data:
        # Convert milliseconds to seconds
        milliseconds_since_epoch = data['publishedAt']
        # Create a datetime object
        published_at_datetime = datetime.fromtimestamp(milliseconds_since_epoch / 1000.0)
        
        # Update the data dictionary with the datetime object
        data['publishedAt'] = published_at_datetime

        print(data['publishedAt'])

    # Store the data in MongoDB
    mongodb_collection.insert_one(data)  # Insert the document into the collection
def main():
    """
    Coordinates the other functions to setup a Kafka Consumer that polls infinitely the broker and a mongodb collection which writes
    the messages received. 
    """
    sleep(5)
    # Setup the consumer
    consumer = consumer_setup()
    # Start consuming messages
    basic_consume_loop(consumer)

if __name__ == "__main__":
    main()
