import pyarrow.parquet as pq
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException
from io import BytesIO
from typing import List, Any, Tuple, Dict
import time
import logging
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("kafka-consumer")
logger.info("Started logging")

class KafkaConsumer:
    """
    Kafka Consumer class: it has everything needed to interface with the Kafka Broker, retrive topics, partitions and messages.
    On instantiation, it will call the initialize_consumer() function in order to contact the kafka broker inside docker and
    establish a connection. 
    Args:
        bootstrap_servers (str): Kafka broker address. Default is 'kafka:9092'.
        group_id (str): Consumer group id. Default is 'mongo-group'.
        client_id (str): Consumer client id. Default is 'mongo-consumer'.
        auto_offset_reset (str): Offset reset policy. Default is 'earliest'.
    Note:
        The following configuration parameters are set by default to maximize throughput:
        heartbeat.interval.ms: 5000 (set it slightly higher than default to limit network overhead)
        session.timeout.ms: 20000  (provide a cushion for longer processing times and prevent unnecessary rebalances due to delayed heartbeats)
        poll.timeout.ms (in confluent kafka seconds): 12 (to avoid unnecessary long blocking while ensuring enough time for batch processing)
    """
    def __init__(self,
                 bootstrap_servers: str = 'kafka:9092',
                 group_id: str = 'mongo-group',
                 client_id: str = 'mongo-consumer',
                 auto_offset_reset: str = 'earliest',):
        
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "client.id" : client_id,
            "heartbeat.interval.ms": 5000, # 5 seconds
            "session.timeout.ms": 20000,   # 20 seconds
        } 

        # Attributes for managing topics and messages
        self.current_topic_list = []
        self.msg = None
        self.consumer = None
        self.poll_timeout = 12.0 # seconds 

        # Initialize the consumer on instantiation
        self.initialize_consumer()
        

    def get_topics(self) -> List[str]:
        """
        Uses the Kafka consumer to obtain metadata from the broker, then extracts the list of topics available.
        Removes internal Kafka topics (like "__consumer_offsets") and subscribes to new topics if found.
        This function is needed for the correct startup of the consumer, as it will subscribe to all topics available.
        Returns:
            List[str]: List of topics extracted from the broker.
        """
        metadata = self.consumer.list_topics(timeout=10.0)
        topics_dict = metadata.topics
        topics = list(topics_dict.keys())
         
        # Remove internal Kafka topic if present
        if '__consumer_offsets' in topics:
            topics.remove('__consumer_offsets')
             
        # Find new topics that aren't in our current list
        difference = list(set(topics) - set(self.current_topic_list))
        
        if difference:
            logger.info(f"Found new topics:\n {difference}")
            # Update our current topic list
            self.current_topic_list.extend(difference)
            # Subscribe to new topics
            self.consumer.subscribe(difference)
            logger.info(f"Subscribed to {difference}")
            
        return topics

    def initialize_consumer(self) -> None:
        """
        Initializes the Kafka consumer by creating an instance and subscribing to topics.
        """
        try:
            self.consumer = Consumer(self.config)
            logger.info(f"Kafka consumer initialized correctly\n")
            self.topic = self.get_topics()  # Call get_topics with self
        except KafkaException as e:
            logger.error(f"Failed to initialize kafka consumer: {str(e)}")
            raise

    def poll_message(self) -> Any:
        """
        Poll Kafka for messages with a specified timeout.
        Args:
            timeout (float): Time in seconds to wait for a message.
        Returns:
            Message object (cimpl.Message) or None if no message is retrieved.
        """
        try:
            msg = self.consumer.poll(self.poll_timeout)
            if msg is None:
                return None
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition: {msg.error()}") 
                    return None
                else:
                    logger.error(f"Error while polling: {msg.error()}") 
                    return None
                    
            return msg
            
        except KafkaException as e:
            logger.error(f"Error in poll: {str(e)}")
            raise

    def close(self):
        """
        Closes the Kafka consumer connection.
        """
        if self.consumer is not None:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except KafkaException as e:
                logger.error(f"Error closing consumer: {str(e)}")
                raise
    
    def get_metadata(self):
        """
        Retrieves metadata from Kafka.
        Args:
            timeout (float): Time in seconds to wait for metadata.
        Returns:
            Metadata object with information about topics and partitions.
        """
        metadata = self.consumer.list_topics(timeout = 10.0)
        return metadata
    
    def consume_messages_spark(self) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        """
        Consumes messages from Kafka for Spark processing and writing.
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
        logger.info(f"Topics in the kafka class are: {topics}") 
        # Initialize data structures
        all_messages = []
        topic_messages = {}  # Changed to store messages per topic
        
        # Validation checks
        if not topics:
            logger.info(f"No topics found in Kafka broker to consume.")
            return [], {}
        logger.info(f"Consuming messages from topics: {topics}") 
        
        # Tracking variables
        total_messages_consumed = 0
        topics_with_messages = set()
        
        try:
            # Subscribe to all topics
            self.consumer.subscribe(topics)
            
            # Continue polling until no more messages
            while True:
                msg = self.poll_message()
                
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
                    logger.error(f"Error decoding message from topic {current_topic}: {decode_error}") 
                    continue
        
        except Exception as e:
            logger.error(f"Unexpected error during Kafka message consumption: {e}") 
        finally:
            summary = "\n".join([
            f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}",
            "--- Kafka Message Consumption Summary ---",
            f"Total messages consumed: {total_messages_consumed}",
            f"Topics with messages: {topics_with_messages}",
            "Detailed message count per topic:"
            ])
            logger.info(summary)
            for topic, messages in topic_messages.items():
                logger.info(f"{topic}: {messages} messages")
        all_messages = self.convert_dates_in_dictionaries(all_messages) 
        logger.info(f"Finally this is the dictionary of topic messages: {topic_messages}") 
        return all_messages, topic_messages

    def decode_parquet(self, msg: Any) -> List[Dict[str, Any]]:
        """
        Decodes a parquet-encoded message and returns it.
        Args:
            msg (Any): Parquet-encoded Kafka message object. The message value is expected to be in a binary format.
        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the decoded message, where each dictionary corresponds to a row of the Parquet data.
        """
        # Use BytesIO to read the binary Parquet data
        buffer = BytesIO(msg)
        pyarrow_schema = pa.schema([
                    ('source', pa.string()),
                    ('text', pa.string()),
                    ('company', pa.string()),
                    ('date', pa.string()),
                    ('tp_stars', pa.int32()), 
                    ('tp_location', pa.string()),
                    ('yt_videoid', pa.string()),
                    ('yt_like_count', pa.int32()),
                    ('yt_reply_count', pa.int32()),
                    ('re_id', pa.string()),
                    ('re_subreddit', pa.string()),
                    ('re_vote', pa.int32()),
                    ('re_reply_count', pa.int32())
                ])
        # Read the Parquet data back into an Arrow table
        table = pq.read_table(source = buffer, schema = pyarrow_schema)
        
        # Convert the Arrow table to a pandas DataFrame for easier manipulation
        decoded_msg = table.to_pylist()

        logger.info(f"Function decode_parquet worked.")
        
        return decoded_msg
    
    def convert_dates_in_dictionaries(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert 'date' field in list of dictionaries to datetime objects.
        Args:
            data (List[Dict[str, Any]]): A list of dictionaries, each containing a 'date' field which is a string representing a date.
        Returns:
            List[Dict[str, Any]]: The updated list of dictionaries with 'date' fields converted to datetime objects.
        """
        logger.info("Entered conversion function")
        for item in data:
            # Handle different potential date formats
            if isinstance(item.get('date'), str):
                try:
                    # Try parsing with multiple potential formats
                    item['date'] = datetime.fromisoformat(item['date'].replace('Z', '+00:00'))
                except ValueError:
                    # Fallback parsing attempts
                    formats = [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d'
                    ]
                    for fmt in formats:
                        try:
                            item['date'] = datetime.strptime(item['date'], fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        # If no format works, log or handle as needed
                        logger.error(f"Could not parse date: {item['date']}")
        return data
