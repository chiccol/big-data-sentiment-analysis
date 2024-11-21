from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
import pandas as pd
from kafka_consumer import KafkaConsumer
import os

def init_kafka(kafka_adv_external_listener, client_id, group_id):
    print("Initializing Kafka consumer...")
    try:
        consumer = KafkaConsumer(bootstrap_servers=kafka_adv_external_listener, 
                                 client_id=client_id, 
                                 group_id=group_id)
    except Exception as e:
        print(f"Error initializing Kafka consumer: {e}")
        exit(1)

    print("Getting data from Kafka...")

    return consumer

def init_spark(spark_master, spark_port):
    spark = SparkSession.builder \
        .master(f"spark://{spark_master}:{spark_port}") \
        .appName("Writer") \
        .getOrCreate()
    return spark

def write_mongo(topic_messages, spark):
    print(topic_messages.keys())
    for collection, messages in topic_messages.items():
        if messages:  # Check if there are messages for the topic
            # Create DataFrame for the topic
            df = spark.createDataFrame(messages)
            print(f"Collection is {collection}")
            
            # Write to MongoDB. db "reviews" is hard coded since it's the only DB we are using.
            df.write \
                .format("mongo") \
                .mode("append") \
                .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/reviews.{collection}") \
                .save()
            
            print(f"Wrote {df.count()} messages from topic {collection} to MongoDB")

def write_postgres(all_messages, spark):
    # Do NLP part. Note that in the init.sql file we have to create a correct table, here we need to define messages correctly. 
    # For now it's just writing random stuff to make sure it works.
    df_all = spark.createDataFrame(all_messages)
    # Show the DataFrame (optional)
    
    print("Writing on postgres")
    url = "jdbc:postgresql://postgres:5432/warehouse"

    properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "predictions"
    # jdbc is the thing we installed to write directly from the dataframe
    df_all.write.jdbc(url=url, table=table_name, mode="append", properties=properties)


def main():
    # Get venv variables 

    spark_master = os.getenv("SPARK_MASTER_HOST")
    spark_port = os.getenv("SPARK_MASTER_PORT")
    batch_size = int(os.getenv("BATCH_SIZE")) 
    print(f"Processing data using batch size: {batch_size}")
    kafka_adv_external_listener = os.getenv("KAFKA_ADVERTISED_LISTENERS")
    client_id = os.getenv("CLIENT_ID")
    group_id = os.getenv("GROUP_ID")

    
    spark = init_spark(spark_master, spark_port)

    consumer = init_kafka(kafka_adv_external_listener, client_id, group_id)

    all_messages, topic_messages = consumer.consume_messages_spark()
   # print("Printing all messages:\n", all_messages, "*"*50 +" \n")
    
   # print("Printing topic messages:\n", topic_messages)
    
    if topic_messages: 
        write_mongo(topic_messages, spark)
    if all_messages:
        write_postgres(all_messages, spark)

    else:
        print("No data was consumed")
        print("Sleeping for 15 seconds...")

if __name__ == "__main__":
    main()
