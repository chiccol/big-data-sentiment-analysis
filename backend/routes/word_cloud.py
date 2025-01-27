from fastapi import APIRouter, HTTPException
from utils.database import mongo_wc
from models.mongo_models import AllWordCloudData
from utils.config import logger

router = APIRouter()


@router.get("/word-cloud-data", response_model=AllWordCloudData) 
def get_all_word_cloud_data():
    """
    Return *all* word-count data for *all* companies, no filtering.
    """
    logger.info("Starting to fetch all word cloud data from MongoDB collections.")
    
    try:
        # List all collections in MongoDB
        all_collections = mongo_wc.list_collection_names()
        logger.debug(f"List of all collections in MongoDB: {all_collections}")

        all_data = []
        # Filter for collections that end with "_word_count"
        for company_name in all_collections:
            logger.info(f"Processing word_count collection: {company_name}")
    
            collection = mongo_wc[company_name]
            docs = list(collection.find({}))
            logger.debug(f"Found {len(docs)} documents in collection '{company_name}'.")

            for doc in docs:
                all_data.append({
                    "company": company_name,
                    "word": doc["word"],
                    "count": doc["count"],
                })

        logger.info(f"Finished processing all word_count collections. Total data items: {len(all_data)}")
        return {"data": all_data}

    except Exception as e:
        logger.error(f"An error occurred while fetching word cloud data: {e}")
        raise HTTPException(status_code=500, detail=str(e))