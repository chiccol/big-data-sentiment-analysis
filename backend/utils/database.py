import os
from pymongo import MongoClient
import psycopg2
from psycopg2 import pool

from fastapi import HTTPException
from utils.config import logger

logger.info("Importing utils.database...")

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "reviews")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_wc = mongo_client["word_count"]
mongo_couples = mongo_client["couples_count"]
mongo_triples = mongo_client["triples_count"]

    
# PostgreSQL Connection Pool
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB_NAME = os.getenv("PG_DB_NAME", "warehouse")

logger.info("Creating PostgreSQL connection pool...")
try:
    pg_pool = pool.SimpleConnectionPool(
        1, 20,  # min and max connections
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB_NAME
    )
    if pg_pool:
        logger.info("PostgreSQL connection pool created successfully.")
except (Exception, psycopg2.DatabaseError) as error:
    logger.error(f"Error creating PostgreSQL connection pool: {error}")
    pg_pool = None


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
