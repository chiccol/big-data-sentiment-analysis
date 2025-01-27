from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import Companies

router = APIRouter()

@router.get("/companies", response_model=Companies)
def get_companies():
    try:
        companies = mongo_db.list_collection_names()
        companies = [company for company in companies if company not in ["word_count", "bigrams", "trigrams", "rag"]]
        return {"companies": companies}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


