from fastapi import APIRouter, HTTPException
from utils.database import mongo_couples
from models.mongo_models import TopBigramsResponse, BigramCount
from utils.config import logger
from pymongo import DESCENDING

router = APIRouter()

@router.get("/top_couples/{company}", response_model=TopBigramsResponse)
def get_top_couples(company: str):
    """
    Retrieve the top 20 bigrams for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 bigrams for company: {company}")

    try:
        # List all collections in the `couples_count` database
        all_collections = mongo_couples.list_collection_names()
        logger.debug(f"Available bigram collections: {all_collections}")

        # Check if the specified company has a corresponding collection
        if company not in all_collections:
            logger.error(f"Collection for company '{company}' does not exist in couples_count.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found in couples_count.")

        # Access the company's bigram collection
        collection = mongo_couples[company]
        logger.debug(f"Accessing bigram collection: {company}")

        # Query top 20 bigrams sorted by count (descending)
        # Note: If your code saves bigram counts under "count" or "sum(count)",
        # adjust this find().sort(...) call accordingly.
        top_bigrams_cursor = collection.find().sort("count", DESCENDING).limit(20)
        
        top_bigrams = []
        for doc in top_bigrams_cursor:
            bigram = doc.get("bigram")
            count = doc.get("count")

            if bigram is not None and count is not None:
                top_bigrams.append(BigramCount(bigram=bigram, count=int(count)))
            else:
                logger.warning(f"Document missing 'bigram' or 'count' fields: {doc}")

        if not top_bigrams:
            logger.warning(f"No bigram data found for company '{company}'.")
            raise HTTPException(status_code=404, detail=f"No bigram data found for company '{company}'.")

        logger.info(f"Retrieved {len(top_bigrams)} top bigrams for company '{company}'.")

        return TopBigramsResponse(company=company, top_bigrams=top_bigrams)

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top bigrams for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")