from pyspark.sql import SparkSession

from kafka_consumer import KafkaConsumer
from utils import write_mongo, write_postgres
import os

def main():
    # Get venv variables 

    spark_master = os.getenv("SPARK_MASTER_HOST")
    spark_port = os.getenv("SPARK_MASTER_PORT")
    kafka_adv_external_listener = os.getenv("KAFKA_ADVERTISED_LISTENERS")
    client_id = os.getenv("CLIENT_ID")
    group_id = os.getenv("GROUP_ID")

    spark = SparkSession.builder \
        .master(f"spark://{spark_master}:{spark_port}") \
        .appName("Writer-Sentiment-Analysis") \
        .getOrCreate()

    print("Initializing Kafka consumer...", flush=True)
    try:
        consumer = KafkaConsumer(bootstrap_servers=kafka_adv_external_listener, 
                                 client_id=client_id, 
                                 group_id=group_id)
    except Exception as e:
        print(f"Error initializing Kafka consumer: {e}", flush=True)
        exit(1)

    print("Getting data from Kafka...", flush=True)

    all_messages, topic_messages = consumer.consume_messages_spark()
   # print("Printing all messages:\n", all_messages, "*"*50 +" \n")
    
   # print("Printing topic messages:\n", topic_messages)
    
    if topic_messages: 
        write_mongo(topic_messages, spark)
    if all_messages:
        write_postgres(all_messages, spark)

    else:
        print("No data was consumed", flush=True)
        print("Sleeping for 15 seconds...", flush=True)

if __name__ == "__main__":
    main()
