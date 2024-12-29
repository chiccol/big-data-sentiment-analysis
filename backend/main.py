import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from database import mongo_db, pg_pool, mongo_wc
import psycopg2.extras
from typing import List, Dict
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from nltk.corpus import stopwords
import re
from collections import Counter
from pymongo import DESCENDING


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


class AggregatedPostgresData(BaseModel):
    date: datetime
    # company: str
    reddit: Optional[float] = None
    trustpilot: Optional[float] = None
    youtube: Optional[float] = None

class AggregatedPostgresResponse(BaseModel):
    aggregated_data: List[AggregatedPostgresData]

    
class WordCloudItem(BaseModel):
    company: str
    word: str
    count: int
    date: str | None = None
    
class AllWordCloudData(BaseModel):
    data: List[WordCloudItem]
    
class Companies(BaseModel):
    companies: List[str]
    
    
class WordCount(BaseModel):
    word: str
    count: int

class TopWordsResponse(BaseModel):
    company: str
    top_words: List[WordCount]


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

    
@app.get("/companies", response_model=Companies)
def get_companies():
    try:
        return {"companies": mongo_db.list_collection_names()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/aggregated-postgres-data/{company}",
         response_model=AggregatedPostgresResponse)
def get_aggregated_postgres_data_discrete(company: str, 
                                          pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """
    Returns an object in the form of AggregatedPostgresResponse,
    where each record's sentiment is calculated as +1 (positive) or -1 (negative),
    then averaged for each date/source/company.
    """
    logger.debug("Fetching daily aggregated data (+1/-1) from PostgreSQL.")
    logger.debug(f"Company ID: {company}")
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Notice we use a CASE expression to convert each row to +1 or -1
        # Then we take the average of those values for each date/company/source
        query = """
            SELECT
                CAST(
                    CASE 
                        WHEN "date" LIKE '%%T%%Z' THEN "date"::timestamp 
                        ELSE "date"::date                                  
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
            WHERE
                company = %s
            GROUP BY
                source, company, normalized_date
            ORDER BY
                normalized_date ASC;
        """

        logger.debug(f"Executing SQL query (daily +1/-1): {query}")
        cursor.execute(query, (company,))
        logger.debug(f"Company passed to query: {company}")
        rows = cursor.fetchall()
        logger.debug(f"Rows fetched: {rows}")
        logger.debug(f"Fetched {len(rows)} rows from PostgreSQL (daily +1/-1).")
        cursor.close()
        pg_pool.putconn(pg_conn)  # or however you return the conn to the pool

        # We'll aggregate the data in a dictionary keyed by (date, company),
        # with separate fields for reddit, trustpilot, youtube
        aggregation = {}
        for row in rows:
            date_str = row['normalized_date'].strftime("%Y-%m-%d")
            source = row['source'].lower()  # ensure consistent naming
            # db_company = row['company']
            daily_score = float(row['daily_sentiment_score'])  # average of +1/-1

            key = f'{date_str}' #f"{date_str}-{db_company}"
            if key not in aggregation:
                aggregation[key] = {
                    "date": row['normalized_date'],  # keep it as datetime
                    # "company": db_company,
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


@app.get("/word-cloud-data", response_model=AllWordCloudData) 
def get_all_word_cloud_data():
    """
    Return *all* word-count data for *all* companies, no filtering.
    """
    logger.info("Starting to fetch all word cloud data from MongoDB collections.")
    
    try:
        # List all collections in MongoDB
        all_collections = mongo_wc.list_collection_names()
        logger.debug(f"List of all collections in MongoDB: {all_collections}")

        all_data = []
        # Filter for collections that end with "_word_count"
        for company_name in all_collections:
            logger.info(f"Processing word_count collection: {company_name}")
    
            collection = mongo_wc[company_name]
            docs = list(collection.find({}))
            logger.debug(f"Found {len(docs)} documents in collection '{company_name}'.")

            for doc in docs:
                all_data.append({
                    "company": company_name,
                    "word": doc["word"],
                    "count": doc["count"],
                })

        logger.info(f"Finished processing all word_count collections. Total data items: {len(all_data)}")
        return {"data": all_data}

    except Exception as e:
        logger.error(f"An error occurred while fetching word cloud data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/top_words/{company}", response_model=TopWordsResponse)
def get_top_words(company: str):
    """
    Retrieve the top 20 words for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 words for company: {company}")

    try:
        # List all collections in MongoDB
        all_collections = mongo_wc.list_collection_names()
        logger.debug(f"Available collections: {all_collections}")

        # Check if the specified company has a corresponding collection
        if company not in all_collections:
            logger.error(f"Collection for company '{company}' does not exist.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")

        # Access the company's word count collection
        collection = mongo_wc[company]
        logger.debug(f"Accessing collection: {company}")

        # Query to get top 20 words sorted by count in descending order
        top_words_cursor = collection.find().sort("count", DESCENDING).limit(20)
        top_words = []

        for doc in top_words_cursor:
            # Validate that 'word' and 'count' fields exist
            word = doc.get("word")
            count = doc.get("count")

            if word is not None and count is not None:
                top_words.append(WordCount(word=word, count=int(count)))
            else:
                logger.warning(f"Document missing 'word' or 'count' fields: {doc}")

        if not top_words:
            logger.warning(f"No word data found for company '{company}'.")
            raise HTTPException(status_code=404, detail=f"No word data found for company '{company}'.")

        logger.info(f"Retrieved {len(top_words)} top words for company '{company}'.")

        return TopWordsResponse(company=company, top_words=top_words)

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top words for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")