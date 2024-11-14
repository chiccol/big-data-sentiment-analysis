import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from mongodb_manager import MongoDB

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
        """Properly close the Kafka consumer."""
        if self.consumer is not None:
            try:
                self.consumer.close()
                print("Kafka consumer closed")
            except KafkaException as e:
                print(f"Error closing consumer: {str(e)}")
                raise
    
    def get_metadata(self, timeout = 10.0):
        metadata = self.consumer.list_topics(timeout = timeout)
        return metadata

    def inspect_broker(self, num_messages: int = 5):
        """
        Inspect the broker to see what topics, partitions, and messages are available.
        Attempts to consume a few messages from each topic partition.
        """
        print("\nInspecting Kafka broker...\n")
        
        # List all topics
        metadata = self.consumer.list_topics(timeout=10)
        topics = metadata.topics.keys()
        
        if not topics:
            print("No topics found in Kafka broker.")
            return
        
        print("Topics available in broker:", topics)
        
        # Inspect each topic's partitions and messages
        for topic in topics:
            if topic == '__consumer_offsets':  # Ignore internal Kafka topic
                continue
            
            print(f"\nInspecting topic: {topic}")
            partitions = metadata.topics[topic].partitions.keys()
            print(f"Partitions for {topic}: {partitions}")
            
            # Try to consume a few messages from each partition
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
            
        print("\nFinished inspecting broker.\n")

    def consume_messages(self,consumer,timeout = 100.0):
        print("Entered consume message function")
        metadata = self.get_metadata(timeout=timeout)
        topics = list(metadata.topics.keys())
        spark_msgs = [] if consumer == "spark" else None

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
                    msg = self.poll_message(timeout=timeout)
                    
                    if msg is None:
                        print("No more messages available.")
                        break
                    elif msg.error():
                        print(f"Error while polling message: {msg.error()}")
                    else:
                        count += 1
                        print(f"The message is of type {type(msg.value())} and it is {msg.value()}")
                        topic = str(msg.topic())
                        msg = self.decode_parquet(msg.value())
                        if isinstance(consumer, MongoDB):
                            print(f"Attempting to write it on mongo on collection {topic}")
                            consumer.write_on_mongo(msg, topic) # mongo should be moved
                        elif consumer == "spark":
                            print(f"Appending message to spark_msgs")
                            spark_msgs.append(msg)
                        else:
                            print("Consumer not recognized")
                            ValueError("Consumer not recognized. Consumer should be either MongoDB object or spark")

    def decode_parquet(self, msg):
        # Use BytesIO to read the binary Parquet data
        buffer = BytesIO(msg)
        
        # Read the Parquet data back into an Arrow table
        table = pq.read_table(buffer)
        
        # Convert the Arrow table to a pandas DataFrame for easier manipulation
        decoded_msg = table.to_pylist()
        print(f"Function decode_parquet worked, this is the output: {decoded_msg}")
        return decoded_msg