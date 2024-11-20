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

all_messages, topic_messages = consumer.consume_messages_spark(consumer="spark")

print("Data received.")

print("Initializing Spark session...")

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

if all_messages:
    
    print("processing data...")
    df = spark.createDataFrame(data, schema)
    # Perform sentiment analysis only for sources without natural labels
    # Show results
    print(df.show(len(all_messages)))

    # Count the number of reviews processed for each source
    df_count_by_source = df.groupBy("source").count()
    print("num of reviews processed:",df_count_by_source.show())

    print("Writing on postgres")
    url = "jdbc:postgresql://postgres:5432/warehouse"

    properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "predictions"
    # jdbc is the thing we installed to write directly from the dataframe
    df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)

    data = [
    ("source1", "skdjaslkjd123!@#", "2024-11-18", 5, "klsdjf^&%", "vid123", 100, 20),
    ("source2", "asjdlajs*(!(@#", None, 3, "asldkjas%", None, 50, 10)
    ]

    gibberish_df = spark.createDataFrame(data, schema)
    gibberish_df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)




    print("Processing completed. Sleeping for 15 seconds...")
else:
    print("No data was consumed")
    print("Sleeping for 15 seconds...")
