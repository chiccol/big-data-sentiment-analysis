from fastapi import APIRouter, HTTPException
import os
from typing import Dict, Any, List
from utils.database import mongo_db

router = APIRouter()


@router.get("/read_summary/{company}")
def read_summary(company: str):
    """
    4. Once done, reads from MongoDB the summarized data for that company.
    5. Returns the data as JSON.
    """

    try:
        collection = mongo_db["rag"]  # Access the 'rag' collection

        # Fetch the document corresponding to the company
        # Assuming each document has a field 'company' matching the company
        # Adjust the field name as per your actual document structure
        document = collection.find_one({"company": company})

        if not document:
            raise HTTPException(status_code=404, detail=f"No summary found for company '{company}'")

        # Optionally, remove the MongoDB-specific '_id' field or convert it to string
        document.pop("_id", None)  # Remove '_id' field if not needed

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MongoDB error: {e}")

    # 5. Return the data
    return {"summary": document["answers"]}
