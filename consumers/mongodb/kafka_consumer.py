from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import List

class KafkaConsumer:
    def __init__(self,
                 bootstrap_servers: str = 'kafka:9092',
                 group_id: str = 'mongoconsumer',
                 auto_offset_reset: str = 'earliest',): 
        
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset
        } 
        self.current_topic_list = []
        self.msg = None
        self.consumer = None
        self.initialize_consumer()

    def get_topics(self) -> List[str]:
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
            
        print("\nFinished inspecting broker.\n")
