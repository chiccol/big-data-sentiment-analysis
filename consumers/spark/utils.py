from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType

import pandas as pd
import torch
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
import os
import logging

from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("spark-master-utils")
logger.info("Started logging")

output_schema = StructType([
        StructField("probabilities", ArrayType(DoubleType()), True),
        StructField("sentiment", StringType(), True)
        ])

# Schema for postgreSQL
schema = StructType([
        StructField("source", StringType(), nullable = False),
        StructField("text", StringType(), nullable = False),
        StructField("company", StringType(), nullable = False),
        StructField("date", TimestampType(), nullable = True),
        StructField("tp_stars", IntegerType(), nullable = True),
        StructField("tp_location", StringType(), nullable = True),
        StructField("yt_videoid", StringType(), nullable = True),
        StructField("yt_like_count", IntegerType(), nullable = True),
        StructField("yt_reply_count", IntegerType(), nullable = True),
        StructField("re_id", StringType(), nullable = True),
        StructField("re_subreddit", StringType(), nullable = True),
        StructField("re_vote", IntegerType(), nullable = True),
        StructField("re_reply_count", IntegerType(), nullable = True)
    ])

checkpoint_path = r"/app/model"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

@pandas_udf(output_schema)
def get_sentiment_udf(text_series: pd.Series) -> pd.DataFrame:
    """
    Uses DistilBert to tokenize and create predictions.
    Args:
        text_series: pd.Series
    Returns:
        results: pd.DataFrame 
    """
    results = []

    # Load DistilBERT tokenizer and model
    # This makes each worker load the model and tokenizer, limiting memory usage but increasing latency
    tokenizer = DistilBertTokenizer.from_pretrained(checkpoint_path)
    model = DistilBertForSequenceClassification.from_pretrained(checkpoint_path)
    model.to(device)
    try:
        batch_size = int(os.getenv("BATCH_SIZE"))
    except:
        logger.info("BATCH_SIZE not set, using default value 32")
        batch_size = 32
    logger.info(f"Processing data using batch size: {batch_size}")

    model.eval()
    with torch.no_grad():
        logger.info(f"Processing {len(text_series)} messages")
        for i in range(0, len(text_series), batch_size):
            batch = list(text_series[i:i + batch_size])
            inputs = tokenizer(batch, return_tensors='pt', truncation=True, padding=True)
                
            outputs = model(**inputs)
            probabilities = torch.softmax(outputs.logits, dim=1)  # Keep as tensor
                
            # Directly compute sentiment labels
            sentiment_labels = ["negative", "neutral", "positive"]
            sentiment_indices = torch.argmax(probabilities, dim=1).tolist()
            sentiments = [sentiment_labels[idx] for idx in sentiment_indices]

            results.extend([{"probabilities": prob.tolist(), "sentiment": label} 
                                for prob, label in zip(probabilities, sentiments)])
        
    return pd.DataFrame(results)

def process_data(all_messages: List[Dict], spark: SparkSession) -> DataFrame:
    """
    Craetes a Spark Dataframe, runs the model and returns a Spark Datafame with the probabilities column.
    The prediction are performed only for non-Trustpilot sources since Trustpilot has a natural label. 
    For Trustpilot, the sentiment is determined as follows:
    - 4 or 5 stars -> positive
    - 3 stars -> neutral
    - 1 or 2 stars -> negative

    Args:
        all_messages -> list of dictionaries
        spark -> Spark session
    Returns:
        df_with_sentiment_multi_columns -> Spark DataFrame with sentiment probabilities
    """

    df = spark.createDataFrame(all_messages, schema)
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
                        when(col("tp_stars") > 3, "positive")
                        .when(col("tp_stars") == 3, "neutral")
                        .otherwise("negative")
                    )
        ) \
        .drop("sentiment_analysis")
    
    # Create separate columns for sentiment probabilities
    df_with_sentiment_multi_columns = df_with_sentiment.withColumn("negative_probability", df_with_sentiment["sentiment_probabilities"].getItem(0)) \
                                            .withColumn("neutral_probability", df_with_sentiment["sentiment_probabilities"].getItem(1)) \
                                            .withColumn("positive_probability", df_with_sentiment["sentiment_probabilities"].getItem(2)) \
                                            .drop(*["sentiment_probabilities"])
    
    return df_with_sentiment_multi_columns

def write_mongo(df_mongo: DataFrame, topics: List[str]) -> None:
    """
    Writes on MongoDB's different collections based on the topic(company) the message came from.
    Args:
        df_mongo -> SparkDataFrame
        topics -> dict with key = topic
    Returns:
        None
    """
    for topic in topics:
        filtered_df_mongo = df_mongo.filter(df_mongo.company == topic)
        filtered_df_mongo.write \
            .format("mongo") \
            .mode("append") \
            .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/reviews.{topic}") \
            .save()
        # Note: this is here for logging, but in a real-world scenario would create futile overhead
        logger.info(f"Wrote {filtered_df_mongo.count()} messages from topic {topic} to MongoDB")
    return None

def write_postgres(df_postgres: DataFrame) -> None:
    """
    Writes on Postgres table.
    Args:
        df_postgres -> Spark DataFrame
    Returns:
        None
    """
    logger.info(f"Writing on postgres")
    url = "jdbc:postgresql://postgres:5432/warehouse"

    properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "predictions"
    # jdbc is the thing we installed to write directly from the dataframe
    df_postgres.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
    return None
