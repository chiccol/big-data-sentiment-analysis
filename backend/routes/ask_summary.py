from fastapi import APIRouter, HTTPException
import socket
import os
from typing import Dict, Any, List, Optional
from utils.database import mongo_db
import json
from utils.config import logger

router = APIRouter()

# Socket details (host and port where the rag script socket server is listening)
RAG_SOCKET_HOST = os.getenv("RAG_SOCKET_HOST", "rag")
RAG_SOCKET_PORT = int(os.getenv("RAG_SOCKET_PORT", "5000"))

@router.get("/trigger_summary/{company}")
def trigger_summary(company: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    1. Connects via socket to the rag container.
    2. Sends the company name.
    3. Waits to receive the "Done" message.
    4. Once done, reads from MongoDB the summarized data for that company.
    5. Returns the data as JSON.
    """

    # 1. Create a socket connection to the rag container
    logger.info(f"Connecting to rag socket at {RAG_SOCKET_HOST}:{RAG_SOCKET_PORT}")
    try:
        rag_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rag_socket.connect((RAG_SOCKET_HOST, RAG_SOCKET_PORT))
        logger.info("Connected to rag socket")
    except Exception as e:
        logger.error(f"Cannot connect to the rag socket: {e}")
        raise HTTPException(status_code=500, detail=f"Cannot connect to the rag socket: {e}")
    
    # prepare the json to send to the rag script
    company_data = {"company": company,
            "sources": ["trustpilot", "youtube", "reddit"],
            "start_date": start_date if start_date else "2024-01-01",
            "end_date": end_date if end_date else "2025-01-01"}
    company_data = json.dumps(company_data)

    # 2. Send the company name to the socket
    logger.info(f"Sending data to rag socket: {company_data}")
    try:
        rag_socket.sendall(company_data.encode("utf-8"))
        logger.info("Data sent to rag socket")
    except Exception as e:
        logger.error(f"Error sending data to socket: {e}")
        rag_socket.close()
        raise HTTPException(status_code=500, detail=f"Error sending data to socket: {e}")

    # 3. Wait until the message "Done" is received
    done_received = False
    try:
        while True:
            data = rag_socket.recv(1024)  # Adjust buffer size as needed
            if not data:
                # No data means the socket was closed unexpectedly
                break
            message = data.decode("utf-8").strip()
            if message == "Done":
                done_received = True
                break
    except Exception as e:
        rag_socket.close()
        raise HTTPException(status_code=500, detail=f"Error reading from socket: {e}")
    finally:
        rag_socket.close()

    if not done_received:
        raise HTTPException(status_code=500, detail="Did not receive 'Done' from the rag script")

    # 4. Read from MongoDB for that company’s summarized data
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
