### DOESN'T WORK YET ###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
import pandas as pd
import torch
from kafka_consumer import KafkaConsumer

import os

spark_master = os.getenv('SPARK_MASTER_HOST')
spark_port = os.getenv('SPARK_MASTER_PORT')
num_records = int(os.getenv('NUM_RECORDS'))

kafka_adv_external_listener = os.getenv('KAFKA_ADVERTISED_LISTENERS')
#kakfka_adv_listeners = kafka_adv_listeners.split(", ")
#external_listener = [kafka_listener for kafka_listener in kakfka_adv_listeners if "EXTERNAL" in kafka_listener][0]
#external_listener = external_listener.split("://")[1]

print("Initializing Kafka consumer...")
try:
    consumer = KafkaConsumer(bootstrap_servers=kafka_adv_external_listener, 
                             client_id="spark-consumer", 
                             group_id="spark-group")
except Exception as e:
    print(f"Error initializing Kafka consumer: {e}")
    exit(1)

print("Getting data from Kafka...")

data = consumer.consume_messages(consumer="spark")

print(data)
print("Data received.")

print("Initializing Spark session...")
# Initialize Spark session
# I could add a if len(data) > 0: to check if there is data to process
spark = SparkSession.builder \
       .master(f"spark://{spark_master}:{spark_port}") \
       .appName("Sentiment Analysis with DistilBERT") \
       .getOrCreate()

schema = StructType([
    StructField("source", StringType(), nullable = False),
    StructField("text", StringType(), nullable = False),
    StructField("date", StringType(), nullable = True),
    StructField("tp-stars", StringType(), nullable = True),
    StructField("tp-location", StringType(), nullable = True),
    StructField("yt-videoid", StringType(), nullable = True),
    StructField("yt-like-count", IntegerType(), nullable = True),
    StructField("yt-reply-count", IntegerType(), nullable = True)
])

model_path = r"/app/model/distilbert-base-uncased"
tokenizer_path = r"/app/model/tokenizer-distilbert-base-uncased"

# Load DistilBERT tokenizer and model
tokenizer = DistilBertTokenizer.from_pretrained(tokenizer_path)
model = DistilBertForSequenceClassification.from_pretrained(model_path)  

# Define a UDF for sentiment analysis
@pandas_udf('array<double>')
def sentiment_udf(text_series):
    results = []
    for text in text_series:
        inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
        outputs = model(**inputs)
        prediction = torch.softmax(outputs.logits, dim=1)[0]
        results.append(prediction.tolist())  # Convert to list for Spark compatibility
    return pd.Series(results)

print("processing data...")
df = spark.createDataFrame(data, schema)
df_with_sentiment = df.withColumn("sentiment_probabilities", sentiment_udf(col("text")))

df_with_sentiment_multi_columns = df_with_sentiment.withColumn("positive", df_with_sentiment["sentiment_probabilities"].getItem(0)) \
                                        .withColumn("neutral", df_with_sentiment["sentiment_probabilities"].getItem(1)) \
                                        .withColumn("negative", df_with_sentiment["sentiment_probabilities"].getItem(2)) \
                                        .drop("sentiment_probabilities")

# Show results
print(df_with_sentiment_multi_columns.take(len(data)))
print("num of reviews processed:",df_with_sentiment_multi_columns.count())
print("Processing completed. Sleeping for 15 seconds...")