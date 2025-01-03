from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import Companies

router = APIRouter()

@router.get("/companies", response_model=Companies)
def get_companies():
    try:
        return {"companies": mongo_db.list_collection_names()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


