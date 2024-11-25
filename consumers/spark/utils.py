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
        StructField("kafka_topic", StringType(), nullable = False),
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
    try:
        batch_size = int(os.getenv("BATCH_SIZE"))
    except:
        print("BATCH_SIZE not set, using default value 32", flush=True)
        batch_size = 32
    print(f"Processing data using batch size: {batch_size}", flush=True)

    model.eval()
    with torch.no_grad():
        print(f"Processing {len(text_series)} messages", flush=True)
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


schema = StructType([
        StructField("source", StringType(), nullable = False),
        StructField("text", StringType(), nullable = False),
        StructField("company", StringType(), nullable = False),
        StructField("date", StringType(), nullable = True),
        StructField("tp_stars", IntegerType(), nullable = True),
        StructField("tp_location", StringType(), nullable = True),
        StructField("yt_videoid", StringType(), nullable = True),
        StructField("yt_like_count", IntegerType(), nullable = True),
        StructField("yt_reply_count", IntegerType(), nullable = True),
    ])

def process_data(all_messages, spark):
    df = spark.createDataFrame(all_messages, schema)
    print("Successfully created a dataframe")
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
    # CHANGED DROP TO DROP(*["sentiment_analysis", "text"]) MAYBE IT WILL WORK
    df_with_sentiment_multi_columns = df_with_sentiment.withColumn("negative_probability", df_with_sentiment["sentiment_probabilities"].getItem(0)) \
                                            .withColumn("neutral_probability", df_with_sentiment["sentiment_probabilities"].getItem(1)) \
                                            .withColumn("positive_probability", df_with_sentiment["sentiment_probabilities"].getItem(2)) \
                                            .drop(*["sentiment_probabilities"])

    # using df_with_sentiment_multi_columns:
    # df_mongo: select what you need
    # df_postgres: drop "text"
    
    return df_with_sentiment_multi_columns
