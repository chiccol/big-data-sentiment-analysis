from fastapi import APIRouter, HTTPException
from utils.database import mongo_wc
from models.mongo_models import TopWordsResponse, WordCount
from utils.config import logger
from pymongo import DESCENDING

router = APIRouter()

@router.get("/top_words/{company}", response_model=TopWordsResponse)
def get_top_words(company: str):
    """
    Retrieve the top 20 words for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 words for company: {company}")

    try:
        # List all collections in MongoDB
        all_collections = mongo_wc.list_collection_names()
        logger.debug(f"Available collections: {all_collections}")

        # Check if the specified company has a corresponding collection
        if company not in all_collections:
            logger.error(f"Collection for company '{company}' does not exist.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")

        # Access the company's word count collection
        collection = mongo_wc[company]
        logger.debug(f"Accessing collection: {company}")

        # Query to get top 20 words sorted by count in descending order
        top_words_cursor = collection.find().sort("count", DESCENDING).limit(20)
        top_words = []

        for doc in top_words_cursor:
            # Validate that 'word' and 'count' fields exist
            word = doc.get("word")
            count = doc.get("count")

            if word is not None and count is not None:
                top_words.append(WordCount(word=word, count=int(count)))
            else:
                logger.warning(f"Document missing 'word' or 'count' fields: {doc}")

        if not top_words:
            logger.warning(f"No word data found for company '{company}'.")
            raise HTTPException(status_code=404, detail=f"No word data found for company '{company}'.")

        logger.info(f"Retrieved {len(top_words)} top words for company '{company}'.")

        return TopWordsResponse(company=company, top_words=top_words)

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top words for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")