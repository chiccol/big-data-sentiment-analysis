from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import LastComment
from utils.config import logger

router = APIRouter()

# function to fetch the last comment for a specified company for every source
@router.get("/last_comment/{company}", response_model=LastComment)
def get_last_comment(company: str):
    """
    Retrieve the last comment for a specified company for every source.
    """
    logger.info(f"Fetching last comment for company: {company}")

    try:
        # Access the MongoDB collection of the company
        company_collection = mongo_db[company]
        logger.debug(f"Accessed MongoDB collection for company '{company}'.")
        
        # Retrieve the last comment for the company from the MongoDB collection for every source
        pipeline = [
            {
                "$sort": {"date": -1}  # Sort documents by date in descending order
            },
            {
                "$group": {
                    "_id": "$source",  # Group by the source field
                    "last_comment": {"$first": "$text"},  # Select the most recent comment
                }
            }
        ]
        
        result = list(company_collection.aggregate(pipeline))
        last_comments_data = {}

        for r in result:
            source = r["_id"].lower()  # Convert source to lowercase to match model fields
            if source in ["reddit", "trustpilot", "youtube"]:  # Ensure the source matches the model
                last_comments_data[source] = r["last_comment"]

        last_comment = LastComment(**last_comments_data)
        
        return last_comment

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        logger.error(f"Error fetching last comment for company '{company}': {http_exc}")
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching last comment for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")