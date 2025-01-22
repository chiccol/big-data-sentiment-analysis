from fastapi import APIRouter, Depends, HTTPException
from utils.database import get_pg_connection, pg_pool
from models.postgres_models import TrustpilotResponse
from utils.config import logger
import psycopg2
from psycopg2 import extras

router = APIRouter()

# Returns data from trustpilot table
@router.get("/trustpilot_data/{company}", response_model=TrustpilotResponse)
def get_trustpilot_data(company: str,
                        pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """Get data from trustpilot table"""
    
    try:
        with pg_conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM trustpilot WHERE company = '{company}'")
            data = cur.fetchall()
            return data
        
    except Exception as e:
        logger.error(f"Error in get_trustpilot_data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e