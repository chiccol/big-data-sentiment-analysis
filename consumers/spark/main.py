from pyspark.sql import SparkSession
from kafka_consumer import KafkaConsumer
from utils import get_sentiment_udf, process_data 
import os

def write_mongo(df_mongo, topics):
    for topic in topics:
        filtered_df_mongo = df_mongo.filter(df_mongo.company == topic)
        filtered_df_mongo.write \
            .format("mongo") \
            .mode("append") \
            .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/reviews.{topic}") \
            .save()
        
        print(f"Wrote {filtered_df_mongo.count()} messages from topic {topic} to MongoDB", flush=True)

def write_postgres(df_postgres):

    print("Writing on postgres", flush=True)
    url = "jdbc:postgresql://postgres:5432/warehouse"

    properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_name = "predictions"
    # jdbc is the thing we installed to write directly from the dataframe
    df_postgres.write.jdbc(url=url, table=table_name, mode="append", properties=properties)

def main():
    # Get venv variables 

    spark_master = os.getenv("SPARK_MASTER_HOST")
    spark_port = os.getenv("SPARK_MASTER_PORT")
    kafka_adv_external_listener = os.getenv("KAFKA_ADVERTISED_LISTENERS")
    client_id = os.getenv("CLIENT_ID")
    group_id = os.getenv("GROUP_ID")

    spark = SparkSession.builder \
        .master(f"spark://{spark_master}:{spark_port}") \
        .config("spark.mongodb.output.uri", f"mongodb://mongo:27017/") \
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

    while True:
        print("Getting data from Kafka...", flush=True)
        all_messages, topics = consumer.consume_messages_spark()
        print("We obtained", topics) 
        print(f"Messages consumed with Spark: {len(all_messages)}", flush=True)
        if all_messages:
            df = process_data(all_messages, spark)
            df.show(5)
            df_mongo = df.select(["source", "date", "text", "company", "sentiment"])
            df_postgres = df.select(["source", "date", "company", "sentiment", "negative_probability", 
                                     "neutral_probability", "positive_probability", "tp_stars", "tp_location", 
                                     "yt_videoid", "yt_like_count", "yt_reply_count"])
            write_mongo(df_mongo, topics)
            write_postgres(df_postgres)
        else:
            print("No data was consumed", flush=True)
            print("Sleeping for 15 seconds...", flush=True)

if __name__ == "__main__":
    main()
