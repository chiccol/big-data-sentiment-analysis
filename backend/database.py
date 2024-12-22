import os
from pymongo import MongoClient
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "reviews")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB_NAME]

    
# PostgreSQL Connection Pool
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB_NAME = os.getenv("PG_DB_NAME", "warehouse")

try:
    pg_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,  # min and max connections
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB_NAME
    )
    if pg_pool:
        print("PostgreSQL connection pool created successfully")
except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL", error)
    pg_pool = None
