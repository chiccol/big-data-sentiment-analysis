import os
import re
import pandas as pd
import logging
from pymongo import MongoClient
from pyspark.sql.functions import col, explode, split, pandas_udf
from pyspark.sql.types import StringType
from nltk.corpus import stopwords

logger = logging.getLogger("spark-wordcount")

STOPWORDS = set(stopwords.words('english'))

def clean_text(text):
    if pd.isnull(text):
        return ''
    # Remove punctuation and digits
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    return text.lower()

@pandas_udf(StringType())
def preprocess_pandas_udf(text_series: pd.Series) -> pd.Series:
    """Applies clean_text to each element of the Series."""
    return text_series.apply(clean_text)

def write_company_word_counts(df, spark):
    """
    Processes the given DataFrame to compute word counts per (company, word),
    then writes them into multiple MongoDB collections under the 'word_count' database.
    """
    logger.info("[WordCount] Entered write_company_word_counts.")

    # Check if the DF is empty
    if not df.head(1):
        logger.warning("[WordCount] DataFrame is empty. Nothing to process.")
        return
    
    # Log some sample rows to confirm we have text and company
    df.select("company", "text").show(5, truncate=False)

    # Preprocess texts
    df_clean = df.withColumn("clean_text", preprocess_pandas_udf(col("text")))

    # Split into words
    words_df = df_clean.select(
        "company",
        explode(split(col("clean_text"), " ")).alias("word")
    )
    logger.info("[WordCount] Exploded DF => row count: {}".format(words_df.count()))

    # Filter out empty strings
    # words_df = words_df.filter(col("word") != "")

    # remove stopwords
    words_df = words_df.filter(~col("word").isin([w for w in STOPWORDS]))

    # Group by (company, word), count occurrences
    word_counts = words_df.groupBy("company", "word").count()
    logger.info("[WordCount] groupBy => row count: {}".format(word_counts.count()))
    
    # Connect to Mongo
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    client = MongoClient(mongo_uri)
    db = client["word_count"]  # requested DB 
    
    companies = df.select("company").distinct().rdd.flatMap(lambda x: x).collect()
    
    # Iterate over collections and load each into a Spark DataFrame
    for company in companies:
        
        # for the first iteration, need to create the collection
        try:
            # Read the collection dynamically using Spark
            old_df = spark.read \
                .format("mongodb") \
                .option("uri", mongo_uri) \
                .option("database", "word_count") \
                .option("collection", company) \
                .load()
            
            word_counts = word_counts.orderBy(col("count").desc()).limit(100)
            logger.info("[WordCount] Top 100 words => row count: {}".format(word_counts.count()))
                
            word_counts_company = word_counts.filter(col("company") == company)
            
            merged_df = old_df.union(word_counts_company)
        
            new_df = merged_df.groupBy("company", "word").sum("count")
            logger.info("[WordCount] groupBy => row count: {}".format(new_df.count()))
        except:
            new_df = word_counts.filter(col("company") == company)
            logger.info("[WordCount] groupBy => row count: {}".format(new_df.count()))

        new_df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option(f"spark.mongodb.output.uri", f"mongodb://mongo:27017/word_count.{company}") \
            .save()
        logger.info(f"[WordCount] Wrote {new_df.count()} word counts to MongoDB collection '{company}'")

    client.close()
    logger.info("[WordCount] Done writing word counts.")
