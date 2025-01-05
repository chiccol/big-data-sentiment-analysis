from fastapi import APIRouter, Depends, HTTPException
from utils.database import get_pg_connection, pg_pool
from models.postgres_models import SuperAggregatedPostgresResponse
from utils.config import logger
import psycopg2

router = APIRouter()

@router.get("/super-aggregated-postgres-data/{company}",
         response_model=SuperAggregatedPostgresResponse)
def get_aggregated_postgres_data_discrete(company: str, 
                                          pg_conn: psycopg2.extensions.connection = Depends(get_pg_connection)):
    """
    Returns an object in the form of AggregatedPostgresResponse,
    where each record's sentiment is calculated as +1 (positive) or -1 (negative),
    then averaged for each date/source/company.
    """
    logger.debug("Fetching daily aggregated data) from PostgreSQL.")
    logger.debug(f"Company ID: {company}")
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # query for fetching daily aggregated data
        query = """
            SELECT
                CAST(
                    CASE 
                        WHEN "date"::TEXT LIKE '%%T%%Z' THEN "date"::timestamp 
                        ELSE "date"::date                                  
                    END AS date
                ) AS normalized_date,
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
                company, normalized_date
            ORDER BY
                normalized_date ASC;
        """

        logger.debug(f"Executing SQL query: {query}")
        cursor.execute(query, (company,))
        logger.debug(f"Company passed to query: {company}")
        rows = cursor.fetchall()
        logger.debug(f"Rows fetched: {rows}")
        logger.debug(f"Fetched {len(rows)} rows from PostgreSQL.")
        cursor.close()
        pg_pool.putconn(pg_conn)  # or however you return the conn to the pool

        # We'll aggregate the data in a dictionary keyed by (date, company),
        # with separate fields for reddit, trustpilot, youtube
        aggregation = {}
        for row in rows:
            date_str = row['normalized_date'].strftime("%Y-%m-%d")
            # db_company = row['company']
            daily_score = float(row['daily_sentiment_score'])  # average of

            key = f'{date_str}' #f"{date_str}-{db_company}"
            if key not in aggregation:
                aggregation[key] = {
                    "date": row['normalized_date'],  # keep it as datetime
                    # "company": db_company,
                    "score": None
                }
            aggregation[key]["score"] = daily_score

        # Sort by date
        aggregated_data = sorted(aggregation.values(), key=lambda x: x['date'])
        logger.info("Successfully processed daily aggregated data.")

        return {"aggregated_data": aggregated_data}

    except Exception as e:
        pg_pool.putconn(pg_conn)
        logger.error(f"Error fetching daily Postgres data: {e}")
        raise HTTPException(status_code=500, detail=str(e))