from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
import pandas as pd
import torch

from transformers import DistilBertTokenizer, DistilBertForSequenceClassification

from kafka_consumer import KafkaConsumer

import os

spark_master = os.getenv('SPARK_MASTER_HOST')
spark_port = os.getenv('SPARK_MASTER_PORT')
num_records = int(os.getenv('NUM_RECORDS'))

kafka_adv_external_listener = os.getenv('KAFKA_ADVERTISED_LISTENERS')

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
    StructField("tp-stars", IntegerType(), nullable = True),
    StructField("tp-location", StringType(), nullable = True),
    StructField("yt-videoid", StringType(), nullable = True),
    StructField("yt-like-count", IntegerType(), nullable = True),
    StructField("yt-reply-count", IntegerType(), nullable = True),
])

model_path = r"/app/model/distilbert-base-uncased"
tokenizer_path = r"/app/model/tokenizer-distilbert-base-uncased"

# Load DistilBERT tokenizer and model
tokenizer = DistilBertTokenizer.from_pretrained(tokenizer_path)
model = DistilBertForSequenceClassification.from_pretrained(model_path)  

output_schema = StructType([
    StructField("probabilities", ArrayType(DoubleType()), True),
    StructField("sentiment", StringType(), True)
])

# Define a UDF for sentiment analysis
@pandas_udf(output_schema)
def get_sentiment_udf(text_series: pd.Series) -> pd.DataFrame:
    results = []
    for text in text_series:
        inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
        outputs = model(**inputs)
        prediction = torch.softmax(outputs.logits, dim=1)[0]
        sentiment_label = ["negative", "neutral", "positive"][torch.argmax(prediction).item()]
        results.append({"probabilities": prediction.tolist(), "sentiment": sentiment_label})  # Convert to list for Spark compatibility
    return pd.DataFrame(results)

print("processing data...")
df = spark.createDataFrame(data, schema)
# Perform sentiment analysis only for sources without natural labels
df_with_sentiment = df.withColumn(
    "sentiment_analysis", 
    when(col("source") != "Trustpilot", get_sentiment_udf(col("text"))).otherwise(None)
)

# Create separate columns for sentiment probabilities and labels
# If the source is Trustpilot, use the star rating to determine sentiment
df_with_sentiment = df_with_sentiment \
    .withColumn("sentiment_probabilities", col("sentiment_analysis.probabilities")) \
    .withColumn("sentiment", 
                when(col("source") != "Trustpilot", col("sentiment_analysis.sentiment"))
                .otherwise(
                    when(col("tp-stars") > 3, "positive")
                    .when(col("tp-stars") == 3, "neutral")
                    .otherwise("negative")
                )
    ) \
    .drop("sentiment_analysis")

df_with_sentiment_multi_columns = df_with_sentiment.withColumn("negative_probability", df_with_sentiment["sentiment_probabilities"].getItem(0)) \
                                        .withColumn("neutral_probability", df_with_sentiment["sentiment_probabilities"].getItem(1)) \
                                        .withColumn("positive_probability", df_with_sentiment["sentiment_probabilities"].getItem(2)) \
                                        .drop("sentiment_probabilities")

# Show results
print(df_with_sentiment_multi_columns.take(len(data)))

# Count the number of reviews processed for each source
df_count_by_source = df.groupBy("source").count()
print("num of reviews processed:",df_count_by_source.show())
print("Processing completed. Sleeping for 15 seconds...")