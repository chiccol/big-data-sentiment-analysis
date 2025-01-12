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
        # Access the last comment collection
        logger.debug(f"Accessing collection: last_comment")
        collection = mongo_db["last_comment"]
        
        # check what companies are in the collection
        companies = collection.distinct("company")
        logger.debug(f"Companies in collection: {companies}")
        
        # Check if the specified company has a corresponding collection
        if company not in companies:
            logger.error(f"Company '{company}' not found.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")
        
        # access the company's document
        company_doc = collection.find_one({"company": company})

        # find the last comment for the company for every source
        # comment is saved in "text" field
        # source is saved in "source" field
        

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        logger.error(f"Error fetching last comment for company '{company}': {http_exc}")
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching last comment for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")