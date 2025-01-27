from fastapi import APIRouter, Depends, HTTPException
from utils.database import get_pg_connection, pg_pool
from models.postgres_models import RedditResponse
from utils.config import logger
import psycopg2
from psycopg2 import extras

router = APIRouter()

# Returns data from reddit table
@router.get("/reddit_data/{company}", response_model=RedditResponse)
def get_company_data(company: str, 
                    pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """Get data from reddit table"""
    
    try:
        with pg_conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(f"""SELECT
                            reddit.vote,
                            reddit.reddit_reply_count,
                            reddit.subreddit,
                            predictions.sentiment
                        FROM
                            reddit
                        INNER JOIN
                            predictions
                        ON
                            reddit.id = predictions.id
                        WHERE
                            predictions.company = {company};""")
            data = cur.fetchall()
            return data
    
    except Exception as e:
        logger.error(f"Error in get_company_data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e
    
    