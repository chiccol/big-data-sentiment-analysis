from fastapi import APIRouter, HTTPException
from utils.database import mongo_triples
from models.mongo_models import TopTrigramsResponse, TrigramCount
from utils.config import logger
from pymongo import DESCENDING

router = APIRouter()

@router.get("/top_triples/{company}", response_model=TopTrigramsResponse)
def get_top_triples(company: str):
    """
    Retrieve the top 20 trigrams for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 trigrams for company: {company}")

    try:
        # List all collections in the `triples_count` database
        all_collections = mongo_triples.list_collection_names()
        logger.debug(f"Available trigram collections: {all_collections}")

        # Check if the specified company has a corresponding collection
        if company not in all_collections:
            logger.error(f"Collection for company '{company}' does not exist in triples_count.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found in triples_count.")

        # Access the company's trigram collection
        collection = mongo_triples[company]
        logger.debug(f"Accessing trigram collection: {company}")

        # Query top 20 trigrams sorted by count (descending)
        top_trigrams_cursor = collection.find().sort("count", DESCENDING).limit(20)
        
        top_trigrams = []
        for doc in top_trigrams_cursor:
            trigram = doc.get("trigram")
            count = doc.get("count")

            if trigram is not None and count is not None:
                top_trigrams.append(TrigramCount(trigram=trigram, count=int(count)))
            else:
                logger.warning(f"Document missing 'trigram' or 'count' fields: {doc}")

        if not top_trigrams:
            logger.warning(f"No trigram data found for company '{company}'.")
            raise HTTPException(status_code=404, detail=f"No trigram data found for company '{company}'.")

        logger.info(f"Retrieved {len(top_trigrams)} top trigrams for company '{company}'.")

        return TopTrigramsResponse(company=company, top_trigrams=top_trigrams)

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top trigrams for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

