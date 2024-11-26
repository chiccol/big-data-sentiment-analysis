import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from io import BytesIO
from typing import List
import time

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
                 auto_offset_reset: str = 'earliest'):
        
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
            print(f"Found new topics:\n {difference}", flush=True)
            # Update our current topic list
            self.current_topic_list.extend(difference)
            # Subscribe to new topics
            self.consumer.subscribe(difference)
            print(f'Subscribed to {difference}', flush=True)
            
        return topics

    def initialize_consumer(self) -> None:
        """
        Init function to correctly instantiate the consumer.
        """
        try:
            self.consumer = Consumer(self.config)
            print('Kafka consumer initialized correctly\n', flush=True)
            self.topic = self.get_topics()  # Call get_topics with self
        except KafkaException as e:
            print(f'Failed to initialize kafka consumer: {str(e)}', flush=True)
            raise

    def poll_message(self, timeout: float = 1.0):
        """Poll Kafka for messages with a timeout."""
        try:
            msg = self.consumer.poll(timeout)
            if msg is None:
                return None
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.error()}", flush=True)
                    return None
                else:
                    print(f"Error while polling: {msg.error()}", flush=True)
                    return None
                    
            return msg
            
        except KafkaException as e:
            print(f"Error in poll: {str(e)}", flush=True)
            raise

    def close(self):
        """Closes the Kafka consumer."""
        if self.consumer is not None:
            try:
                self.consumer.close()
                print("Kafka consumer closed", flush=True)
            except KafkaException as e:
                print(f"Error closing consumer: {str(e)}", flush=True)
                raise
    
    def get_metadata(self, timeout = 10.0):
        """
        Returns: metadata Kafka object.
        """
        metadata = self.consumer.list_topics(timeout = timeout)
        return metadata


# VERIFY THAT timeout for poll aligns with heartbeat.interval.ms and session.timeout.ms
    def consume_messages_spark(self, timeout=15.0):
        """
        Consumes messages from Kafka for Spark processing and writing.
        
        Args:
            timeout (float): Seconds to wait between polls.
        
        Returns:
            tuple: (
                all_messages: list of all messages regardless of topic,
                topic_messages: dict with topic as key and list of messages as value
            )
        """
        ignore_topics = ['__consumer_offsets']
        
        # Get metadata and create a list of topics to explore
        metadata = self.get_metadata()
        topics = [topic for topic in metadata.topics.keys() if topic not in ignore_topics]
        print(f"Topics in the kafka class are: {topics}")        
        
        # Initialize data structures
        all_messages = []
        topic_messages = {}  # Changed to store messages per topic
        
        # Validation checks
        if not topics:
            print("No topics found in Kafka broker to consume.", flush=True)
            return [], {}
        
        print(f"Consuming messages from topics: {topics}", flush=True)
        
        # Tracking variables
        total_messages_consumed = 0
        topics_with_messages = set()
        
        try:
            # Subscribe to all topics
            self.consumer.subscribe(topics)
            
            # Continue polling until no more messages
            while True:
                msg = self.poll_message(timeout=timeout)
                
                # Break if no more messages
                if msg is None:
                    break
                
                try:
                    # Decode message 
                    current_topic = str(msg.topic())
                    msg_data = self.decode_parquet(msg.value())
                    
                    # Store messages by topic and in all_messages
                    if current_topic not in topic_messages:
                        topic_messages[current_topic] = 0
                    topic_messages[current_topic] += len(msg_data)
                    all_messages.extend(msg_data)
                    
                    # Update tracking variables
                    total_messages_consumed += len(msg_data)
                    if topic_messages[current_topic] > 0:
                        topics_with_messages.add(current_topic)
                    
                except Exception as decode_error:
                    print(f"Error decoding message from topic {current_topic}: {decode_error}", flush=True)
                    continue
        
        except Exception as e:
            print(f"Unexpected error during Kafka message consumption: {e}", flush=True)
        
        finally:
            # Logging summary
            print(f"Time:{time.localtime()}\n--- Kafka Message Consumption Summary ---", flush=True)
            print(f"Total messages consumed: {total_messages_consumed}", flush=True)
            print(f"Topics with messages: {topics_with_messages}", flush=True)
            print("Detailed message count per topic:", flush=True)
            for topic, messages in topic_messages.items():
                print(f"{topic}: {messages} messages", flush=True)
        
        print(f"Finally this is the dictionary of topic messages: {topic_messages}", flush=True)
        return all_messages, topic_messages

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
        print(f"Function decode_parquet worked.", flush=True)
        return decoded_msg
