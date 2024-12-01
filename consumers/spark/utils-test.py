from pyspark.sql.functions import when, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

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

def spark_process(all_messages, spark):
    print("Spark print of all messages what is the schema???\n", all_messages)
    df = spark.createDataFrame(all_messages, schema)
    df.show(5)
    
    return df

def write_postgres(all_messages, spark):
    # Do NLP part. Note that in the init.sql file we have to create a correct table, here we need to define messages correctly. 
    # For now it's just writing random stuff to make sure it works.
    df = spark.createDataFrame(all_messages, schema)
    print("Successfully created DataFrame", flush=True)
    # Show the DataFrame (optional)
    df_with_sentiment = process_data(df)
    print("Writing on postgres", flush=True)
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
    print(topic_messages.keys(), flush=True)
    for collection, messages in topic_messages.items():
        if messages:  # Check if there are messages for the topic
            # Create DataFrame for the topic
            df = spark.createDataFrame(messages)
            print(f"Collection is {collection}", flush=True)

            df = df.select(["source", "date", "text", "kafka_topic"])
            
            # Write to MongoDB. db "reviews" is hard coded since it's the only DB we are using.
            df.write \
                .format("mongo") \
                .mode("append") \
                .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/reviews.{collection}") \
                .save()
            
            print(f"Wrote {df.count()} messages from topic {collection} to MongoDB", flush=True)
