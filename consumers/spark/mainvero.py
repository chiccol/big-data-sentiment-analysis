from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
import pandas as pd
from kafka_consumer import KafkaConsumer
import os

spark_master = os.getenv("SPARK_MASTER_HOST")
spark_port = os.getenv("SPARK_MASTER_PORT")
batch_size = int(os.getenv("BATCH_SIZE")) 
print(f"Processing data using batch size: {batch_size}")
kafka_adv_external_listener = os.getenv("KAFKA_ADVERTISED_LISTENERS")

print("Initializing Kafka consumer...")
try:
    consumer = KafkaConsumer(bootstrap_servers=kafka_adv_external_listener, 
                             client_id="spark-consumer", 
                             group_id="spark-group")
except Exception as e:
    print(f"Error initializing Kafka consumer: {e}")
    exit(1)

print("Getting data from Kafka...")

all_messages, topic_messages = consumer.consume_messages_spark()

print("Data received.")

spark = SparkSession.builder \
        .master(f"spark://{spark_master}:{spark_port}") \
        .appName("Sentiment Analysis with DistilBERT") \
        .config("spark.mongodb.input.uri", "mongodb://mongo:27017/mydatabase.mycollection") \
        .config("spark.mongodb.output.uri", "mongodb://mongo:27017/mydatabase.mycollection") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1') \
        .getOrCreate()

schema = StructType([
        StructField("source", StringType(), nullable = False),
        StructField("text", StringType(), nullable = False),
        StructField("date", StringType(), nullable = True),
        StructField("tp-stars", IntegerType(), nullable = True),
        StructField("tp-location", StringType(), nullable = True),
        StructField("yt-videoid", StringType(), nullable = True),
        StructField("yt-like-count", IntegerType(), nullable = True),
        StructField("yt-reply-count", IntegerType(), nullable = True),
    ])

# Process and write data for each topic
if topic_messages:
    for topic, messages in topic_messages.items():
        print(f"\nProcessing messages for topic: {topic}")
        
        if not messages:  # Skip if no messages for this topic
            print(f"No messages to process for topic {topic}")
            continue
            
        try:
            # Create DataFrame for this topic's messages
            df_topic = spark.createDataFrame(messages, schema)
            df_topic.show() 
            # Configure MongoDB connection for this specific topic
            mongodb_uri = f"mongodb://mongo:27017/reviews.{topic}"
            
            print(f"Writing data to MongoDB collection: {topic}")
            df_topic.write \
                .format("mongodb") \
                .mode("append") \
                .option("spark.mongodb.output.uri", mongodb_uri) \
                .save()
                
            print(f"Successfully wrote {df_topic.count()} records to collection {topic}")
            
        except Exception as e:
            print(f"Error processing topic {topic}: {e}")
            continue
            
    print("\nFinished processing all topics")
else:
    print("No messages received from Kafka")
