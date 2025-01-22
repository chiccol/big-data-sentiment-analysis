from fastapi import APIRouter, HTTPException, Depends
from utils.database import get_pg_connection
from models.postgres_models import YoutubeResponse
from utils.config import logger
import psycopg2
from psycopg2 import extras

router = APIRouter()

@router.get("/youtube_data/{company}", response_model=YoutubeResponse)
def get_youtube_data(company: str,
                     pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """Get data from youtube table"""
    
    try:
        with pg_conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM youtube WHERE company = '{company}'")
            data = cur.fetchall()
            return data
        
    except Exception as e:
        logger.error(f"Error in get_youtube_data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e