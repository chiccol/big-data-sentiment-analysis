from confluent_kafka import Producer
from time import sleep

class KafkaProducer:
    def __init__(self, bootstrap_servers, client_id):
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id
            }
        self.producer = Producer(producer_config)

    def produce(self, record, topic):
        def delivery_report(err, msg):
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

        self.producer.produce(topic, value=record, callback=delivery_report)
        self.producer.poll(1)  # Wait up to 1 second for any delivery reports (optional but recommended)
        sleep(0.1)  # Sleep for a short time to allow the message to be delivered