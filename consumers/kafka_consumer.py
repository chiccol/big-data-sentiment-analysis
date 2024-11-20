import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from io import BytesIO
from typing import List

class KafkaConsumer:
    """
    Kafka Consumer class: it has everything needed to interface with the Kafka Broker, retrive topics, partitions and messages.
    On instantiation, it will call the initialize_consumer() function in order to contact the kafka broker inside docker and
    establish a connection. 
    TODO pass bootstrap_servers, group_id, auto_offset_reset etc as environment parameters in the docker compose.
    """
    def __init__(self,
                 bootstrap_servers: str = 'kafka:9092',
                 group_id: str = 'mongo-group',
                 client_id: str = 'mongo-consumer',
                 auto_offset_reset: str = 'earliest',): 
        
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            "client.id" : client_id
        } 
        self.current_topic_list = []
        self.msg = None
        self.consumer = None
        self.initialize_consumer()

    def get_topics(self) -> List[str]:
        """
        Uses the kafka consumer to obtain metadata from the broker, then extracts the list of topics available, removes
        "__consumer_offsets" and uses a set operation to check if there are any new topics, if there are, it subscribes to them.
        This function is needed for the correct startup of the consumer.

        Args:
            self

        Returns:
            list of topics extracted from the broker

        """
        metadata = self.consumer.list_topics(timeout=10.0)
        topics_dict = metadata.topics
        topics = list(topics_dict.keys())
         
        # Remove internal Kafka topic if present
        if '__consumer_offsets' in topics:  # Note: double underscore
            topics.remove('__consumer_offsets')
             
        # Find new topics that aren't in our current list
        difference = list(set(topics) - set(self.current_topic_list))
        
        if difference:
            print(f"Found new topics:\n {difference}")
            # Update our current topic list
            self.current_topic_list.extend(difference)
            # Subscribe to new topics
            self.consumer.subscribe(difference)
            print(f'Subscribed to {difference}')
            
        return topics

    def initialize_consumer(self) -> None:
        """
        Init function to correctly instantiate the consumer.
        """
        try:
            self.consumer = Consumer(self.config)
            print('Kafka consumer initialized correctly\n')
            self.topic = self.get_topics()  # Call get_topics with self
        except KafkaException as e:
            print(f'Failed to initialize kafka consumer: {str(e)}')
            raise

    def poll_message(self, timeout: float = 1.0):
        """Poll Kafka for messages with a timeout."""
        try:
            msg = self.consumer.poll(timeout)
            if msg is None:
                return None
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.error()}")
                    return None
                else:
                    print(f"Error while polling: {msg.error()}")
                    return None
                    
            return msg
            
        except KafkaException as e:
            print(f"Error in poll: {str(e)}")
            raise

    def close(self):
        """Closes the Kafka consumer."""
        if self.consumer is not None:
            try:
                self.consumer.close()
                print("Kafka consumer closed")
            except KafkaException as e:
                print(f"Error closing consumer: {str(e)}")
                raise
    
    def get_metadata(self, timeout = 10.0):
        """
        Returns: metadata Kafka object.
        """
        metadata = self.consumer.list_topics(timeout = timeout)
        return metadata

    def consume_messages_spark(self, timeout=100.0):
        """
        Consumes messages from Kafka for Spark processing and writing. This function makes Spark write the messages.

        Args:
            timeout: seconds to wait between polls.

        Returns:
            tuple: (
                all_messages: list of all messages regardless of topic,
                topic_messages: dict with topic as key and list of messages as value
            )
        """
        print("Entered consume messages for Spark")
        metadata = self.get_metadata()
        topics = list(metadata.topics.keys())
        # Initialize data structures
        all_messages = []  # List for all messages
        topic_messages = {}  # Dictionary for topic-specific messages
        
        if not topics:
            print("No topics found in Kafka broker.")
            return None, None
            
        print("Topics available in broker:", topics)
        for topic in topics:
            if topic == '__consumer_offsets':  # Ignore internal Kafka topic
                continue
            
            # Initialize empty list for this topic
            topic_messages[topic] = []
            
            print(f"\nRetrieving messages from {topic}")
            partitions = metadata.topics[topic].partitions.keys()
            print(f"Partitions for {topic}: {partitions}")
            
            for partition in partitions:
                print(f"Reading messages from topic '{topic}', partition {partition}")
                
                while True:
                    msg = self.poll_message(timeout=timeout)
                    
                    if msg is None:
                        print(f"No more messages available for topic {topic}, partition {partition}")
                        break
                        
                    elif msg.error():
                        print(f"Error while polling message: {msg.error()}")
                        continue
                        
                    else:
                        print(f"Message received of type {type(msg.value())}")
                        current_topic = str(msg.topic())
                        msg_data = self.decode_parquet(msg.value())
                        
                        # Add topic information to each message
                        for record in msg_data:
                            record['kafka_topic'] = current_topic
                            
                        # Initialize topic list if it doesn't exist
                        if current_topic not in topic_messages:
                            topic_messages[current_topic] = []
                        
                        # Add messages to both data structures
                        all_messages.extend(msg_data)
                        topic_messages[current_topic].extend(msg_data)
                        
                        print(f"Added {len(msg_data)} messages from topic {current_topic}")
        
        # Remove any topics that ended up with no messages
        topic_messages = {k: v for k, v in topic_messages.items() if v}
        print(topic_messages)
        
        return all_messages, topic_messages

    def consume_messages_mongo(self, mongo_manager, timeout=100.0):
        """
        Consumes messages from Kafka and writes directly to MongoDB.
        Requires a mongo_manager instance for writing operations, mongo handles the writing.
        Currently not used as Spark handles all the writing.
        """
        print("Entered consume messages for MongoDB")
        if mongo_manager is None:
            raise ValueError("mongo_manager is required for MongoDB consumer")

        metadata = self.get_metadata(timeout=timeout)
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
                print(f"Reading messages from topic '{topic}', partition {partition}")
                messages_processed = 0
                
                while True:
                    msg = self.poll_message(timeout=timeout)
                    
                    if msg is None:
                        print(f"No more messages available for topic {topic}, partition {partition}")
                        print(f"Processed {messages_processed} messages for this partition")
                        break
                        
                    elif msg.error():
                        print(f"Error while polling message: {msg.error()}")
                        continue
                        
                    else:
                        current_topic = str(msg.topic())
                        msg_data = self.decode_parquet(msg.value())
                        
                        try:
                            print(f"Writing messages to MongoDB collection {current_topic}")
                            mongo_manager.write_on_mongo(msg_data, current_topic)
                            messages_processed += len(msg_data)
                        except Exception as e:
                            print(f"Error writing to MongoDB: {e}")
                            continue

        print("Finished processing all messages for MongoDB")
        return

    def decode_parquet(self, msg):
        """
        Decodes a parquet-encoded message and returns it.

        Args:
            msg: parquet-encoded messages

        Returns:
            decoded_msg: kafka message object
        """
        # Use BytesIO to read the binary Parquet data
        buffer = BytesIO(msg)
        
        # Read the Parquet data back into an Arrow table
        table = pq.read_table(buffer)
        
        # Convert the Arrow table to a pandas DataFrame for easier manipulation
        decoded_msg = table.to_pylist()
        print(f"Function decode_parquet worked.")
        return decoded_msg
