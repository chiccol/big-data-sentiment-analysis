import os
import re
import pandas as pd
import logging
from pymongo import MongoClient
# from pymongo import MongoClient
from pyspark.sql.functions import (
    col, explode, split, pandas_udf,
    expr, collect_list, struct, map_from_entries,
    create_map, lit, row_number
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType
from nltk.corpus import stopwords
from pyspark.sql.window import Window

logger = logging.getLogger("spark-wordcount")

STOPWORDS = set(stopwords.words('english'))

def clean_text(text):
    if pd.isnull(text):
        return ''
    # Remove punctuation and digits
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    # remove \n
    text = text.replace('\n', ' ')
    return text.lower()

@pandas_udf(StringType())
def preprocess_pandas_udf(text_series: pd.Series) -> pd.Series:
    """Applies clean_text to each element of the Series."""
    return text_series.apply(clean_text)



def write_company_word_counts(df: DataFrame, spark: SparkSession) -> None:
    """
    Processes the given DataFrame to compute word counts per (company, word),
    then writes them into multiple MongoDB collections under the 'reviews' database.
    
    Args:
        df (DataFrame): The input Spark DataFrame containing 'company' and 'text' columns.
        spark (SparkSession): The active Spark session.
    
    Notes:
        - Words are extracted, cleaned, and stopwords are removed.
        - Results are aggregated and saved in MongoDB under 'word_count', 'bigrams', and 'trigrams'.
    """
    logger.info("[WordCount] Entered write_company_word_counts.")

    # Check if the DF is empty
    if df.head(1) == []:
        logger.warning("[WordCount] DataFrame is empty. Nothing to process.")
        return
        
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
    
    # Connect to Mongo
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    # client = MongoClient(mongo_uri)
    # db = client["reviews"]  # requested DB 
    
    companies = df.select("company").distinct().rdd.flatMap(lambda x: x).collect()
    
    logger.info(f"[WordCount] Writing on database word counts, bigrams and trigrams for companies: {companies}.")
    
    # read the present collection
    try:
        spark_word_db = spark.read \
            .format("mongo") \
            .option("uri", mongo_uri) \
            .option("database", "reviews") \
            .option("collection", "word_count") \
            .load()
        logger.info(f"[WordCount] Loaded existing word counts => row count: {spark_word_db.count()}")
    except:
        spark_word_db = spark.createDataFrame([], schema=word_counts.schema)
        logger.info("[WordCount] Created new word counts DataFrame.")
        
    
    # merge the dataframes
    # if empty, just use the word_counts
    if spark_word_db.count() == 0:
        merged_df = word_counts
    else:
        logger.info("[WordCount] Dataframe exploding ... ")
        field_names = spark_word_db.schema["word_counts"].dataType.fieldNames()
        key_value_pairs = []
        for field in field_names:
            key_value_pairs.extend([lit(field), col(f"word_counts.{field}")])
        spark_word_db = spark_word_db.withColumn(
            "word_counts_map",
            create_map(*key_value_pairs)
        )
        spark_word_db = spark_word_db.select(
                col("company"),
                explode(col("word_counts_map")).alias("word", "count")
        )
        logger.info("[WordCount] Merging dataframes.")
        spark_word_db = spark_word_db.drop("_id")
        merged_df = spark_word_db.union(word_counts)
    
    # aggregate the data
    merged_df = merged_df.groupBy("company", "word").sum("count")
    merged_df = merged_df.withColumnRenamed("sum(count)", "count")
    
    # take the top 100 words
    window = Window.partitionBy("company").orderBy(col("count").desc())
    merged_df = merged_df.withColumn("rank", row_number().over(window))
    merged_top100 = merged_df.filter(col("rank") <= 100).drop("rank")
    
    # Transform to have word_counts as a map
    grouped_df = merged_top100.groupBy("company") \
        .agg(collect_list(struct(col("word"), col("count"))).alias("word_counts_list"))

    final_df = grouped_df.withColumn("word_counts", map_from_entries(col("word_counts_list"))) \
        .drop("word_counts_list")
        
    # write the data
    final_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", "reviews") \
        .option("collection", "word_count") \
        .save()
    
    logger.info("[WordCount] Done writing word counts.")
    
    
     # bigrams:
    bigrams_df = df_clean.filter(expr("size(words_array) > 1")).select(
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
    
    try:
        spark_bigram_db = spark.read \
            .format("mongo") \
            .option("uri", mongo_uri) \
            .option("database", "reviews") \
            .option("collection", "bigrams") \
            .load()
        logger.info(f"[WordCount] Loaded existing bigram counts => row count: {spark_bigram_db.count()}")
    except:
        spark_bigram_db = spark.createDataFrame([], schema=bigrams_count.schema)
        logger.info("[WordCount] Created new bigram counts DataFrame.")
        
    # merge the dataframes
    # if empty, just use the bigrams_count
    if spark_bigram_db.count() == 0:
        merged_df = bigrams_count
    else:
        logger.info("[WordCount] Dataframe exploding ... ")
        logger.info(f"Schema: {spark_bigram_db.schema}")
        field_names = spark_bigram_db.schema["bigram_counts"].dataType.fieldNames()
        key_value_pairs = []
        for field in field_names:
            key_value_pairs.extend([lit(field), col(f"bigram_counts.{field}")])
        spark_bigram_db = spark_bigram_db.withColumn(
            "bigram_counts_map",
            create_map(*key_value_pairs)
        )
        spark_bigram_db = spark_bigram_db.select(
            col("company"),
            explode(col("bigram_counts_map")).alias("bigram", "count")
        )
        logger.info("[WordCount] Merging dataframes.")
        spark_bigram_db = spark_bigram_db.drop("_id")
        merged_df = spark_bigram_db.union(bigrams_count)
    
    # aggregate the data
    merged_df = merged_df.groupBy("company", "bigram").sum("count")
    merged_df = merged_df.withColumnRenamed("sum(count)", "count")
    
    # take the top 100 words
    window = Window.partitionBy("company").orderBy(col("count").desc())
    merged_df = merged_df.withColumn("rank", row_number().over(window))
    merged_top100 = merged_df.filter(col("rank") <= 100).drop("rank")
    
    # Transform to have word_counts as a map
    grouped_df = merged_top100.groupBy("company") \
        .agg(collect_list(struct(col("bigram"), col("count"))).alias("bigram_counts_list"))

    final_df = grouped_df.withColumn("bigram_counts", map_from_entries(col("bigram_counts_list"))) \
        .drop("bigram_counts_list")
    
    # write the data
    final_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", "reviews") \
        .option("collection", "bigrams") \
        .save()
    logger.info("[WordCount] Done writing bigrams.") 
    
    # trigrams:
    trigrams_df = df_clean.filter(expr("size(words_array) > 2")).select(
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
        
    try:
        spark_trigram_db = spark.read \
            .format("mongo") \
            .option("uri", mongo_uri) \
            .option("database", "reviews") \
            .option("collection", "trigrams") \
            .load()
        logger.info(f"[WordCount] Loaded existing trigram counts => row count: {spark_trigram_db.count()}")
    except:
        spark_trigram_db = spark.createDataFrame([], schema=trigrams_count.schema)
        logger.info("[WordCount] Created new trigram counts DataFrame.")
        
    # merge the dataframes
    # if empty, just use the trigrams_count
    if spark_trigram_db.count() == 0:
        merged_df = trigrams_count
    else:
        logger.info("[WordCount] Dataframe exploding ... ")
        field_names = spark_trigram_db.schema["trigram_count"].dataType.fieldNames()
        key_value_pairs = []
        for field in field_names:
            key_value_pairs.extend([lit(field), col(f"trigram_count.{field}")])
        spark_trigram_db = spark_trigram_db.withColumn(
            "trigram_counts_map",
            create_map(*key_value_pairs)
        )
        spark_trigram_db = spark_trigram_db.select(
            col("company"),
            explode(col("trigram_counts_map")).alias("trigram", "count")
        )
        logger.info("[WordCount] Merging dataframes.")
        spark_trigram_db = spark_trigram_db.drop("_id")
        merged_df = spark_trigram_db.union(trigrams_count)
    
    # aggregate the data
    merged_df = merged_df.groupBy("company", "trigram").sum("count")
    merged_df = merged_df.withColumnRenamed("sum(count)", "count")

    # take the top 100 words
    window = Window.partitionBy("company").orderBy(col("count").desc())
    merged_df = merged_df.withColumn("rank", row_number().over(window))
    merged_top100 = merged_df.filter(col("rank") <= 100).drop("rank")

    # Transform to have word_counts as a map
    grouped_df = merged_top100.groupBy("company") \
        .agg(collect_list(struct(col("trigram"), col("count"))).alias("trigram_count_list"))

    final_df = grouped_df.withColumn("trigram_count", map_from_entries(col("trigram_count_list"))) \
        .drop("trigram_count_list")
    
    # write the data
    final_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", "reviews") \
        .option("collection", "trigrams") \
        .save()
    
    logger.info("[WordCount] Done writing trigrams.")

    # client.close()