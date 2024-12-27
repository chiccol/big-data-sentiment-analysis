import os
import re
import pandas as pd
import logging
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
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
    total_rows = df.count()  # forces an action
    logger.info(f"[WordCount] df.count() => {total_rows}")
    if total_rows == 0:
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
    words_df = words_df.filter(col("word") != "")

    # (Optional) remove stopwords
    # words_df = words_df.filter(~col("word").isin([w.lower() for w in STOPWORDS]))

    # Group by (company, word), count occurrences
    word_counts = words_df.groupBy("company", "word").count()
    logger.info("[WordCount] groupBy => row count: {}".format(word_counts.count()))

    # Collect results
    results = word_counts.collect()
    if not results:
        logger.warning("[WordCount] No word counts found after grouping. Exiting.")
        return

    # Connect to Mongo
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    client = MongoClient(mongo_uri)
    db = client["word_count"]  # your requested DB name

    # Prepare upserts
    company_updates = defaultdict(list)
    for row in results:
        comp = row["company"]
        word = row["word"]
        count_value = row["count"]

        # We'll store one doc per (word), upserting count
        # For each company, we have a separate collection
        company_updates[comp].append(
            UpdateOne(
                {"word": word}, 
                {
                    "$inc": {"count": count_value},
                    "$setOnInsert": {"company": comp}
                },
                upsert=True
            )
        )

    # Bulk write to each collection
    for comp, update_ops in company_updates.items():
        collection = db[comp]
        logger.info(f"[WordCount] Writing {len(update_ops)} updates to '{comp}' collection.")
        if update_ops:
            res = collection.bulk_write(update_ops)
            logger.info(f"[WordCount] Upserted: {res.upserted_count}, Matched: {res.matched_count}, Modified: {res.modified_count}")
            
            # Index on "word" for uniqueness
            collection.create_index([("word", 1)], unique=True)

    client.close()
    logger.info("[WordCount] Done writing word counts.")
