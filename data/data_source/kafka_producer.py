from confluent_kafka import Producer
from time import sleep
import logging
from typing import Any

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)

logger = logging.getLogger("kafka-producer")
logger.info("Started logging")

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        """
        Initializes the Kafka producer with provided bootstrap servers and client ID.
        Args:
            bootstrap_servers (str): The address of the Kafka brokers.
            client_id (str): A unique identifier for the client instance.
        """
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id
        }
        self.producer = Producer(producer_config)

    def produce(self, record: Any, topic: str) -> None:
        """
        Sends a message (record) to the specified Kafka topic and waits for delivery confirmation.
        Args:
            record (Any): The message to be sent to Kafka, can be any serializable type.
            topic (str): The Kafka topic where the message should be sent.
        """
        def delivery_report(err, msg):
            """
            Callback function to handle delivery reports from the Kafka producer.
            Args:
                err (Error or None): Error object if message delivery fails, None if successful.
                msg (Message): Kafka message that was delivered or failed to deliver.
            """
            if err is not None:
                logger.error(f"Message delivery failed: {err}")
            else:
                logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

        self.producer.produce(topic, value=record, callback=delivery_report)        
        # Poll for delivery reports (optional but recommended)
        self.producer.poll(1)
        # Flush the producer to ensure all messages are delivered before proceeding
        self.producer.flush()
        # Sleep for a short time to allow the message to be delivered
        sleep(0.1)