from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import TopBigramsResponse, BigramCount
from utils.config import logger
import re

router = APIRouter()

@router.get("/top_couples/{company}", response_model=TopBigramsResponse)
def get_top_couples(company: str):
    """
    Retrieve the top 20 bigrams for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 bigrams for company: {company}")

    try:
        # Access the bigram count collection
        logger.debug(f"Accessing collection: bigrams")
        collection = mongo_db["bigrams"]
        
        # check what companies are in the collection
        companies = collection.distinct("company")
        logger.debug(f"Companies in collection: {companies}")
        
        # Check if the specified company has a corresponding collection
        if company not in companies:
            logger.error(f"Company '{company}' not found.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")
        
        # access the company's document
        company_doc = collection.find_one({"company": company})
        
        # access the company's bigram count map
        company_count = company_doc.get("bigram_counts")
        
        # bigram count map contains the top 100 bigrams for the company, schema is {bigram: count}
        # take the top 20 bigrams based on their count
        top_bigrams = []
        # check if there are less than 20 bigrams
        if len(company_count) < 20:
            # take all bigrams
            for bigram, count in company_count.items():
                if re.fullmatch(r"\w+ \w+", bigram):
                    top_bigrams.append(BigramCount(bigram=bigram, count=count))
        else:
            for bigram, count in sorted(company_count.items(), key=lambda x: x[1], reverse=True)[:20]:
                if re.fullmatch(r"\w+ \w+", bigram):
                    top_bigrams.append(BigramCount(bigram=bigram, count=count))

        return TopBigramsResponse(company=company, top_bigrams=top_bigrams)

    except HTTPException as http_exc:
        logger.error(f"Error fetching top bigrams for company '{company}': {http_exc}")
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top bigrams for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")