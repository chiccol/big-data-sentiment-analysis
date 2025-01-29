from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import TopTrigramsResponse, TrigramCount
from utils.config import logger
from pymongo import DESCENDING
import re

router = APIRouter()

@router.get("/top_triples/{company}", response_model=TopTrigramsResponse)
def get_top_triples(company: str):
    """
    Retrieve the top 20 trigrams for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 trigrams for company: {company}")

    try:
        # Access the trigram count collection
        logger.debug(f"Accessing collection: trigrams")
        collection = mongo_db["trigrams"]
        
        # check what companies are in the collection
        companies = collection.distinct("company")
        logger.debug(f"Companies in collection: {companies}")

        # Check if the specified company has a corresponding collection
        if company not in companies:
            logger.error(f"Company '{company}' not found.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")
        
        # access the company's document
        company_doc = collection.find_one({"company": company})
        
        # access the company's trigram count map
        company_count = company_doc.get("trigram_count")
        
        # trigram count map contains the top 100 trigrams for the company, schema is {trigram: count}
        # take the top 20 trigrams based on their count
        top_trigrams = []
        # check if there are less than 20 trigrams
        if len(company_count) < 20:
            # take all trigrams
            for trigram, count in company_count.items():
                if re.fullmatch(r"\w+ \w+ \w+", trigram):
                    top_trigrams.append(TrigramCount(trigram=trigram, count=count))
        else:
            for trigram, count in sorted(company_count.items(), key=lambda x: x[1], reverse=True)[:20]:
                if re.fullmatch(r"\w+ \w+ \w+", trigram):
                    top_trigrams.append(TrigramCount(trigram=trigram, count=count))

        return TopTrigramsResponse(company=company, top_trigrams=top_trigrams)

    except HTTPException as http_exc:
        logger.error(f"Error fetching top trigrams for company '{company}': {http_exc}")
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top trigrams for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

