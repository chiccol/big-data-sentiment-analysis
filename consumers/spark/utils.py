from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
import pandas as pd
import torch
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification

import os

output_schema = StructType([
        StructField("probabilities", ArrayType(DoubleType()), True),
        StructField("sentiment", StringType(), True)
        ])

schema = StructType([
        StructField("source", StringType(), nullable = False),
        StructField("text", StringType(), nullable = False),
        StructField("date", StringType(), nullable = True),
        StructField("tp_stars", IntegerType(), nullable = True),
        StructField("tp_location", StringType(), nullable = True),
        StructField("yt_videoid", StringType(), nullable = True),
        StructField("yt_like_count", IntegerType(), nullable = True),
        StructField("yt_reply_count", IntegerType(), nullable = True),
    ])

# These paths should be replaced in the future with online registry paths
model_path = r"/app/model/distilbert-base-uncased"
tokenizer_path = r"/app/model/tokenizer-distilbert-base-uncased"

@pandas_udf(output_schema)
def get_sentiment_udf(text_series: pd.Series) -> pd.DataFrame:
    results = []

    # Load DistilBERT tokenizer and model
    # This makes each worker load the model and tokenizer, limiting memory usage but increasing latency
    tokenizer = DistilBertTokenizer.from_pretrained(tokenizer_path)
    model = DistilBertForSequenceClassification.from_pretrained(model_path)  
    batch_size = int(os.getenv("BATCH_SIZE")) 
    print(f"Processing data using batch size: {batch_size}")

    model.eval()
    with torch.no_grad():
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


def process_data(df):
    
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

    df_with_sentiment_multi_columns = df_with_sentiment.withColumn("negative_probability", df_with_sentiment["sentiment_probabilities"].getItem(0)) \
                                            .withColumn("neutral_probability", df_with_sentiment["sentiment_probabilities"].getItem(1)) \
                                            .withColumn("positive_probability", df_with_sentiment["sentiment_probabilities"].getItem(2)) \
                                            .drop(["sentiment_probabilities", "text"])
    
    return df_with_sentiment_multi_columns

def write_postgres(all_messages, spark):
    # Do NLP part. Note that in the init.sql file we have to create a correct table, here we need to define messages correctly. 
    # For now it's just writing random stuff to make sure it works.
    df = spark.createDataFrame(all_messages)
    # Show the DataFrame (optional)
    df_with_sentiment = process_data(df)
    print("Writing on postgres")
    url = "jdbc:postgresql://postgres:5432/warehouse"

    properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "predictions"
    # jdbc is the thing we installed to write directly from the dataframe
    df_with_sentiment.write.jdbc(url=url, table=table_name, mode="append", properties=properties)

def write_mongo(topic_messages, spark):
    print(topic_messages.keys())
    for collection, messages in topic_messages.items():
        if messages:  # Check if there are messages for the topic
            # Create DataFrame for the topic
            df = spark.createDataFrame(messages)
            print(f"Collection is {collection}")

            df = df.select(["source", "date", "text", "kafka_topic"])
            
            # Write to MongoDB. db "reviews" is hard coded since it's the only DB we are using.
            df.write \
                .format("mongo") \
                .mode("append") \
                .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/reviews.{collection}") \
                .save()
            
            print(f"Wrote {df.count()} messages from topic {collection} to MongoDB")