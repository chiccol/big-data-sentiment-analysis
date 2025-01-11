from fastapi import APIRouter, Depends, HTTPException
from utils.database import mongo_db
from utils.config import logger
import psycopg2
from psycopg2 import extras


router = APIRouter()

# this function return the average sentiment score for a company
@router.get("/avg-sentiment/{company}")
def avg_sentiment(company: str):
    
    logger.debug(f"Fetching average sentiment score for {company} from MongoDB.")
    # get the collection of the company
    collection = mongo_db[company]
    
    # get the total average sentiment score
    total_avg = collection.aggregate([
        {
            "$group": {
                "_id": "total",
                "avg": {
                    "$avg": {
                        "$cond": [
                            {"$eq": ["$sentiment", "positive"]}, 1,
                            {"$cond": [
                                {"$eq": ["$sentiment", "neutral"]}, 0,
                                -1
                            ]}
                        ]
                    }
                }
            }
        }
    ])
    logger.debug(f"Total average sentiment score for {company} fetched.")
    
    source_avg = collection.aggregate([
        {
            "$group": {
                "_id": "$source",  # Group by source
                "avg": {
                    "$avg": {
                        "$cond": [
                            {"$eq": ["$sentiment", "positive"]}, 1,
                            {"$cond": [
                                {"$eq": ["$sentiment", "neutral"]}, 0,
                                -1
                            ]}
                        ]
                    }
                }
            }
        }
    ])
    logger.debug(f"Average sentiment score for each source for {company} fetched.")
    
    # put the results in a dictionary
    result = {
        "total": list(total_avg),
        "source": list(source_avg)
    }
    
    return result