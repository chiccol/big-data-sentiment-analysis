from fastapi import APIRouter, Depends, HTTPException
from utils.database import get_pg_connection, pg_pool
from models.postgres_models import DailyCountResponse
from utils.config import logger
import psycopg2
from psycopg2 import extras 

router = APIRouter()

# function to compute how many data have been collected every day for the last 30 days
@router.get("/interaction-number/{company}",
            response_model=DailyCountResponse)
def get_interaction_number(company: str,
                           pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """
    Returns an object in the form of DailyCountResponse,
    where each record's count is the number of data collected for each date/company.
    """
    
    logger.debug("Fetching daily count of interactions from PostgreSQL.")
    logger.debug(f"Company ID: {company}")
    
    # check if pg_pool is None
    if pg_pool is None:
        logger.error("No PostgreSQL connection available.")
        raise HTTPException(status_code=500, detail="No PostgreSQL connection available.")
    # check if pg_conn is None
    if pg_conn is None:
        logger.error("No PostgreSQL connection available.")
        raise HTTPException(status_code=500, detail="No PostgreSQL connection available.")
    
    try:
        cursor = pg_conn.cursor(cursor_factory=extras.RealDictCursor)
        # query to fetch daily count of interactions
        query = """
            SELECT
                "date"::date AS normalized_date,
                COUNT(*) AS daily_count
            FROM
                predictions
            WHERE
                company = %s
                AND "date"::date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY
                normalized_date
            ORDER BY
                normalized_date ASC;
        """
        cursor.execute(query, (company,))
        rows = cursor.fetchall()
        cursor.close()
        return {"daily_counts": rows}
    
    except Exception as e:
        logger.error(f"Error fetching daily count of interactions from PostgreSQL: {e}")
        raise HTTPException(status_code=500, detail="Error fetching daily count of interactions from PostgreSQL.")

