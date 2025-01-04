from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import TopWordsResponse, WordCount
from utils.config import logger

router = APIRouter()

@router.get("/top_words/{company}", response_model=TopWordsResponse)
def get_top_words(company: str):
    """
    Retrieve the top 20 words for a specified company based on their count.
    """
    logger.info(f"Fetching top 20 words for company: {company}")

    try:
               
        # Access the word count collection
        logger.debug(f"Accessing collection: word_count")
        collection = mongo_db["word_count"]
        
        # check what companies are in the collection
        companies = collection.distinct("company")
        logger.debug(f"Companies in collection: {companies}")
        
        # Check if the specified company has a corresponding collection
        if company not in companies:
            logger.error(f"Company '{company}' not found.")
            raise HTTPException(status_code=404, detail=f"Company '{company}' not found.")
        
        # access the company's document
        company_doc = collection.find_one({"company": company})
        
        # access the company's word count map
        company_count = company_doc.get("word_counts")

        # word count map contains the top 100 words for the company, schema is {word: count}
        # take the top 20 words based on their count
        top_words = []
        # check if there are less than 20 words
        if len(company_count) < 20:
            # take all words
            for word, count in company_count.items():
                top_words.append(WordCount(word=word, count=count))
        else:
            for word, count in sorted(company_count.items(), key=lambda x: x[1], reverse=True)[:20]:
                top_words.append(WordCount(word=word, count=count))

        return TopWordsResponse(company=company, top_words=top_words)

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        logger.error(f"Error fetching top words for company '{company}': {http_exc}")
        raise http_exc

    except Exception as e:
        logger.error(f"Error fetching top words for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")