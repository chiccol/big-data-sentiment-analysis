import os
import re
import pandas as pd
import logging
from pymongo import MongoClient
from pyspark.sql.functions import (
    col, explode, split, pandas_udf,
    expr, size, sequence, concat_ws
)
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

    # preprocess text
    df_clean = df.withColumn("clean_text", preprocess_pandas_udf(col("text")))
    df_clean = df_clean.withColumn("words_array", split(col("clean_text"), " "))

    # Explode single words
    words_df = df_clean.select(
        "company",
        explode(col("words_array")).alias("word")
    )
    logger.info("[WordCount] Exploded DF => row count: {}".format(words_df.count()))

    # remove stopwords
    words_df = words_df.filter((col("word") != "") & (~col("word").isin([w for w in STOPWORDS])))

    # Group by (company, word), count occurrences
    word_counts = words_df.groupBy("company", "word").count()
    logger.info("[WordCount] groupBy => row count: {}".format(word_counts.count()))
    
    # bigrams:
    bigrams_df = df_clean.select(
        "company",
        expr("""
            transform(
                sequence(0, size(words_array) - 2),
                i -> concat(words_array[i], ' ', words_array[i+1])
            ) as bigrams
        """)
    ).withColumn("bigram", explode(col("bigrams")))
    
    bigrams_count = bigrams_df.groupBy("company", "bigram").count()
    logger.info("[WordCount] Bigram groupBy => row count: {}".format(bigrams_count.count()))
    
    # trigrams:
    trigrams_df = df_clean.select(
        "company",
        expr("""
            transform(
                sequence(0, size(words_array) - 3),
                i -> concat(words_array[i], ' ', words_array[i+1], ' ', words_array[i+2])
            ) as trigrams
        """)
    ).withColumn("trigram", explode(col("trigrams")))
    
    trigrams_count = trigrams_df.groupBy("company", "trigram").count()
    logger.info("[WordCount] Trigram groupBy => row count: {}".format(trigrams_count.count()))
    
    
    # Connect to Mongo
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    client = MongoClient(mongo_uri)
    db = client["word_count"]  # requested DB 
    
    companies = df.select("company").distinct().rdd.flatMap(lambda x: x).collect()
    
    logger.info(f"[WordCount] Writing on database word counts, bigrams and trigrams for companies: {companies}.")
    
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
    
        # writing for bigrams
        try:
            old_df = spark.read \
                .format("mongodb") \
                .option("uri", mongo_uri) \
                .option("database", "couples_count") \
                .option("collection", company) \
                .load()
            
            bigrams_company = bigrams_count.filter(col("company") == company)
            bigrams_top = bigrams_company.orderBy(col("count").desc()).limit(100)
            
            merged_df = old_df.union(bigrams_top)
            new_df = merged_df.groupBy("company", "bigram").sum("count")
            logger.info(f"[WordCount] Bigram counts merged => row count: {new_df.count()}")
        except:
            new_df = bigrams_count.filter(col("company") == company).orderBy(col("count").desc()).limit(100)
            logger.info(f"[WordCount] Bigram counts created => row count: {new_df.count()}")
        
        new_df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("spark.mongodb.output.uri", f"mongodb://mongo:27017/couples_count.{company}") \
            .save()

        logger.info(f"[WordCount] Wrote bigram counts for company '{company}' to MongoDB.")
        
        #trigrams writing
        try:
            old_df = spark.read \
                .format("mongodb") \
                .option("uri", mongo_uri) \
                .option("database", "triples_count") \
                .option("collection", company) \
                .load()
            
            trigrams_company = trigrams_count.filter(col("company") == company)
            trigrams_top = trigrams_company.orderBy(col("count").desc()).limit(100)
            
            merged_df = old_df.union(trigrams_top)
            new_df = merged_df.groupBy("company", "trigram").sum("count")
            logger.info(f"[WordCount] Trigram counts merged => row count: {new_df.count()}")
        except:
            new_df = trigrams_count.filter(col("company") == company).orderBy(col("count").desc()).limit(100)
            logger.info(f"[WordCount] Trigram counts created => row count: {new_df.count()}")
            
        new_df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("spark.mongodb.output.uri", f"mongodb://mongo:27017/triples_count.{company}") \
            .save()
            
        logger.info(f"[WordCount] Wrote trigram counts for company '{company}' to MongoDB.")


    client.close()
    logger.info("[WordCount] Done writing word counts.")
