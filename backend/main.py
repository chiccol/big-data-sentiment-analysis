import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from database import mongo_db, pg_pool
import psycopg2.extras
from typing import List, Dict
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from nltk.corpus import stopwords
import re
from collections import Counter

# JUST TO GENERATE SAMPLE WORD CLOUD DATA
from generate_wordcloud_data import seed_word_count_db
seed_word_count_db()

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
logger.debug("Loading environment variables...")
load_dotenv(dotenv_path=os.path.join(os.getcwd(), "backend/backend.env"))
logger.debug("Environment variables loaded successfully.")

app = FastAPI()

origins = [
    "http://localhost:3000"  # React dev server
]

# Allow CORS
logger.debug("Setting up CORS middleware.")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define Pydantic models
class MongoData(BaseModel):
    source: str
    text: str
    date: datetime
    company: str
    sentiment: str

class AggregatedPostgresData(BaseModel):
    date: datetime
    company: str
    reddit: Optional[float] = None
    trustpilot: Optional[float] = None
    youtube: Optional[float] = None

class AggregatedPostgresResponse(BaseModel):
    aggregated_data: List[AggregatedPostgresData]

class PostgresData(BaseModel):
    id: int
    source: str
    date: datetime
    company: str
    sentiment: str
    negative_probability: Optional[float] = None
    neutral_probability: Optional[float] = None
    positive_probability: Optional[float] = None
    tp_stars: Optional[int] = None
    tp_location: Optional[str] = None
    yt_video_id: Optional[str] = None
    yt_likes: Optional[int] = None
    yt_reply_count: Optional[int] = None
    re_id: Optional[str] = None
    re_vote: Optional[int] = None
    re_reply_count: Optional[int] = None
    re_subreddit: Optional[str] = None

class MongoResponse(BaseModel):
    mongo_data: List[MongoData]

class PostgresResponse(BaseModel):
    postgres_data: List[PostgresData]
    
class WordCloudItem(BaseModel):
    company: str
    word: str
    count: int
    date: str | None = None
    
class AllWordCloudData(BaseModel):
    data: List[WordCloudItem]


@app.get("/", response_model=Dict[str, str])
def read_root():
    logger.debug("Root endpoint accessed.")
    return {"message": "Welcome to the FastAPI prototype!"}

def get_pg_connection() -> psycopg2.extensions.connection:
    """Get a connection from the Postgres connection pool."""
    logger.debug("Attempting to obtain a PostgreSQL connection from the pool...")
    try:
        conn = pg_pool.getconn()
        if conn:
            logger.debug("PostgreSQL connection obtained successfully.")
            return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.get("/mongo-data", response_model=MongoResponse)
def get_mongo_data():
    logger.debug("Fetching data from MongoDB.")
    try:
        all_data = []
        for collection_name in mongo_db.list_collection_names():
            logger.debug(f"Processing collection: {collection_name}")
            if collection_name.startswith("system."):
                logger.debug(f"Skipping system collection: {collection_name}")
                continue

            collection = mongo_db[collection_name]
            data = list(collection.find({}, {"_id": 0}))
            # logger.debug(f"Fetched {len(data)} documents from collection: {collection_name}")
            all_data.extend(data)

        logger.info(f"Successfully fetched {len(all_data)} total documents from MongoDB.")
        return {"mongo_data": all_data}
    except Exception as e:
        logger.error(f"Error fetching MongoDB data: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data from MongoDB")

@app.get("/postgres-data", response_model=PostgresResponse)
def get_postgres_data(pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    logger.debug("Fetching data from PostgreSQL.")
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        logger.debug("Executing SQL query: SELECT * FROM predictions;")
        cursor.execute("SELECT * FROM predictions;")
        rows = cursor.fetchall()
        logger.debug(f"Fetched {len(rows)} rows from PostgreSQL.")
        cursor.close()
        pg_pool.putconn(pg_conn)
        logger.debug("PostgreSQL connection returned to pool.")
        return {"postgres_data": rows}
    except Exception as e:
        pg_pool.putconn(pg_conn)
        logger.error(f"Error fetching PostgreSQL data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/aggregated-postgres-data", response_model=AggregatedPostgresResponse)
def get_aggregated_postgres_data(pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    logger.debug("Fetching aggregated data from PostgreSQL.")
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT
                CAST(
                    CASE 
                        WHEN date LIKE '%T%Z' THEN date::timestamp 
                        ELSE date::date                                  
                    END AS date
                ) AS normalized_date,
                source,
                company,
                AVG(COALESCE(positive_probability,0) - COALESCE(negative_probability,0)) AS average_sentiment_score
            FROM
                predictions
            GROUP BY
                source, company, date
            ORDER BY
                date ASC;
        """
        logger.debug(f"Executing SQL query: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.debug(f"Aggregated {len(rows)} rows from PostgreSQL.")
        cursor.close()
        pg_pool.putconn(pg_conn)

        aggregation = {}
        for row in rows:
            date = row['normalized_date'].strftime("%Y-%m-%d")
            source = row['source'].lower()
            company = row['company']
            avg_score = float(row['average_sentiment_score'])

            key = f"{date}-{company}"
            if key not in aggregation:
                aggregation[key] = {
                    "date": date,
                    "company": company,
                    "reddit": None,
                    "trustpilot": None,
                    "youtube": None
                }
            aggregation[key][source] = avg_score

        aggregated_data = sorted(aggregation.values(), key=lambda x: x['date'])
        logger.info("Successfully processed aggregated data.")
        return {"aggregated_data": aggregated_data}
    except Exception as e:
        pg_pool.putconn(pg_conn)
        logger.error(f"Error fetching aggregated Postgres data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/aggregated-postgres-data-discrete", response_model=AggregatedPostgresResponse)
def get_aggregated_postgres_data_discrete(pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """
    Returns an object in the form of AggregatedPostgresResponse,
    where each record's sentiment is calculated as +1 (positive) or -1 (negative),
    then averaged for each date/source/company.
    """
    logger.debug("Fetching daily aggregated data (+1/-1) from PostgreSQL.")
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Notice we use a CASE expression to convert each row to +1 or -1
        # Then we take the average of those values for each date/company/source
        query = """
            SELECT
                CAST(
                    CASE 
                        WHEN date LIKE '%T%Z' THEN date::timestamp 
                        ELSE date::date                                  
                    END AS date
                ) AS normalized_date,
                source,
                company,
                AVG(
                    CASE 
                        WHEN sentiment = 'positive' THEN 1 
                        WHEN sentiment = 'negative' THEN -1
                        WHEN sentiment = 'neutral'  THEN 0
                    END
                ) AS daily_sentiment_score
            FROM
                predictions
            GROUP BY
                source, company, date
            ORDER BY
                date ASC;
        """

        logger.debug(f"Executing SQL query (daily +1/-1): {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.debug(f"Fetched {len(rows)} rows from PostgreSQL (daily +1/-1).")
        cursor.close()
        pg_pool.putconn(pg_conn)  # or however you return the conn to the pool

        # We'll aggregate the data in a dictionary keyed by (date, company),
        # with separate fields for reddit, trustpilot, youtube
        aggregation = {}
        for row in rows:
            date_str = row['normalized_date'].strftime("%Y-%m-%d")
            source = row['source'].lower()  # ensure consistent naming
            company = row['company']
            daily_score = float(row['daily_sentiment_score'])  # average of +1/-1

            key = f"{date_str}-{company}"
            if key not in aggregation:
                aggregation[key] = {
                    "date": row['normalized_date'],  # keep it as datetime
                    "company": company,
                    "reddit": None,
                    "trustpilot": None,
                    "youtube": None
                }
            aggregation[key][source] = daily_score

        # Sort by date
        aggregated_data = sorted(aggregation.values(), key=lambda x: x['date'])
        logger.info("Successfully processed daily +1/-1 aggregated data.")

        return {"aggregated_data": aggregated_data}

    except Exception as e:
        pg_pool.putconn(pg_conn)
        logger.error(f"Error fetching daily +1/-1 Postgres data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

from pymongo import MongoClient
mongo_client = MongoClient("mongodb://mongo:27017/")
mongo_db = mongo_client["word_count"]


@app.get("/companies")
def get_companies():
    try:
        all_collections = mongo_db.list_collection_names()
        valid_companies = []
        logging.info(f"Found collections: {all_collections}")

        for collection_name in all_collections:
            # Only consider collections that end with "_word_count"
            if collection_name.endswith("_word_count"):
                # Remove the suffix "_word_count"
                base_name = collection_name[:-11]  # e.g., "apple" from "apple_word_count"
                # You could also do base_name.capitalize() or .title() if desired
                valid_companies.append(base_name)

        # Return as a JSON list
        return {"companies": valid_companies}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/word-cloud-data", response_model=AllWordCloudData)  # Or remove if you prefer no validation
def get_all_word_cloud_data():
    """
    Return *all* word-count data for *all* companies, no filtering.
    """
    logger.info("Starting to fetch all word cloud data from MongoDB collections.")
    
    try:
        # List all collections in MongoDB
        all_collections = mongo_db.list_collection_names()
        logger.debug(f"List of all collections in MongoDB: {all_collections}")

        all_data = []
        # Filter for collections that end with "_word_count"
        for collection_name in all_collections:
            logger.info(f"Processing word_count collection: {collection_name}")
            
            # Extract the company name from the collection, e.g. "apple" from "apple_word_count"
            company_name = collection_name
            
            collection = mongo_db[collection_name]
            docs = list(collection.find({}))
            logger.debug(f"Found {len(docs)} documents in collection '{collection_name}'.")

            for doc in docs:
                date_value = doc.get("date")
                if isinstance(date_value, datetime):
                    date_value = date_value.isoformat()

                all_data.append({
                    "company": company_name,
                    "word": doc["word"],
                    "count": doc["count"],
                    "date": date_value
                })

        logger.info(f"Finished processing all word_count collections. Total data items: {len(all_data)}")
        return {"data": all_data}

    except Exception as e:
        logger.error(f"An error occurred while fetching word cloud data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
