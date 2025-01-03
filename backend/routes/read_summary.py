from fastapi import APIRouter, HTTPException
from utils.database import mongo_db
from models.mongo_models import SummaryModel, InfoSummary, SourceSummary, Summary
from utils.config import logger, SOCKET_HOST, SOCKET_PORT, SOCKET_TIMEOUT
import asyncio
from datetime import datetime
import time

router = APIRouter()

SOCKET_HOST = "localhost"       
SOCKET_PORT = 12345             
SOCKET_TIMEOUT = 10             

@router.get("/summary/{company}", response_model=SummaryModel)
async def get_summary(company: str):
    """
    Retrieve the summary for a specified company from the MongoDB 'rag' collection.
    """
    try:
        # Access the 'rag' collection in MongoDB
        collection = mongo_db['rag']
        
        # Query to find the document where 'rag.company.name' matches the provided company name
        document = collection.find_one({'rag.company.name': company})
        
        if not document:
            logger.error(f"Company '{company}' not found in the 'rag' collection.")
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Extract the 'rag.company' sub-document
        rag = document.get('rag', {})
        company_data = rag.get('company', {})
        
        if not company_data:
            logger.error(f"No company data found for '{company}'.")
            raise HTTPException(status_code=404, detail="Company data not found")
        
        # Extract and construct the InfoSummary
        info_data = company_data.get('info', {})
        info_summary = InfoSummary(
            from_date=info_data.get('from_date')
        )
        
        # Helper function to create SourceSummary from source data
        def create_source_summary(source_dict):
            return SourceSummary(
                positive=Summary(str=source_dict.get('positive', "")),
                negative=Summary(str=source_dict.get('negative', "")),
                neutral=Summary(str=source_dict.get('neutral', ""))
            )
        
        # Extract and construct SourceSummary for each source
        youtube_data = company_data.get('youtube', {})
        reddit_data = company_data.get('reddit', {})
        trustpilot_data = company_data.get('trustpilot', {})
        
        youtube_summary = create_source_summary(youtube_data)
        reddit_summary = create_source_summary(reddit_data)
        trustpilot_summary = create_source_summary(trustpilot_data)
        
        # Construct the final SummaryModel
        summary_model = SummaryModel(
            company=company,
            info=info_summary,
            youtube=youtube_summary,
            reddit=reddit_summary,
            trustpilot=trustpilot_summary
        )
        
        return summary_model
    
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        raise http_exc
    except Exception as e:
        logger.error(f"Error fetching summary for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/ask_summary/{company}", response_model=SummaryModel)
async def ask_summary(company: str):
    """
    Sends the company name to the 'rag' container via a socket, waits for a 'Done' response,
    and then retrieves the summary from MongoDB.
    
    Args:
        company (str): The name of the company to process and retrieve the summary for.
    
    Returns:
        SummaryModel: The summary data for the specified company.
    
    Raises:
        HTTPException: If there are issues with socket communication or data retrieval.
    """
    try:
        logger.info(f"Initiating ask_summary for company: {company}")

        # Establish an asynchronous connection to the socket
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SOCKET_HOST, SOCKET_PORT),
            timeout=SOCKET_TIMEOUT
        )
        logger.debug(f"Connected to socket at {SOCKET_HOST}:{SOCKET_PORT}")

        # Send the company name followed by a newline (assuming line-based protocol)
        message = f"{company}\n"
        writer.write(message.encode())
        await writer.drain()
        logger.debug(f"Sent company name to socket: {message.strip()}")

        # Wait for the "Done" response
        response_str = None
        while response_str != "Done":
            response = await reader.readline()
            response_str = response.decode().strip()
            logger.debug(f"Received response from socket: {response_str}")
            time.sleep(1)

        # Close the socket connection
        writer.close()
        await writer.wait_closed()
        logger.debug("Socket connection closed.")

        logger.info(f"Received 'Done' from socket for company: {company}. Fetching summary from DB.")

        # Fetch the summary from MongoDB, similar to get_summary
        collection = mongo_db['rag']
        document = collection.find_one({'rag.company.name': company})

        if not document:
            logger.error(f"Company '{company}' not found in the 'rag' collection after processing.")
            raise HTTPException(status_code=404, detail="Company not found")

        rag = document.get('rag', {})
        company_data = rag.get('company', {})

        if not company_data:
            logger.error(f"No company data found for '{company}' after processing.")
            raise HTTPException(status_code=404, detail="Company data not found")

        # Extract and construct the InfoSummary
        info_data = company_data.get('info', {})
        info_summary = InfoSummary(
            from_date=info_data.get('from_date')
        )

        # Helper function to create SourceSummary from source data
        def create_source_summary(source_dict):
            return SourceSummary(
                positive=Summary(str=source_dict.get('positive', "")),
                negative=Summary(str=source_dict.get('negative', "")),
                neutral=Summary(str=source_dict.get('neutral', ""))
            )

        # Extract and construct SourceSummary for each source
        youtube_data = company_data.get('youtube', {})
        reddit_data = company_data.get('reddit', {})
        trustpilot_data = company_data.get('trustpilot', {})

        youtube_summary = create_source_summary(youtube_data)
        reddit_summary = create_source_summary(reddit_data)
        trustpilot_summary = create_source_summary(trustpilot_data)

        # Construct the final SummaryModel
        summary_model = SummaryModel(
            company=company,
            info=info_summary,
            youtube=youtube_summary,
            reddit=reddit_summary,
            trustpilot=trustpilot_summary
        )

        logger.info(f"Successfully retrieved summary for company: {company}")
        return summary_model

    except asyncio.TimeoutError:
        logger.error(f"Timeout while communicating with the 'rag' container for company '{company}'.")
        raise HTTPException(status_code=504, detail="Timeout while processing the request")
    except ConnectionRefusedError:
        logger.error(f"Could not connect to the 'rag' container at {SOCKET_HOST}:{SOCKET_PORT}.")
        raise HTTPException(status_code=502, detail="Bad gateway: Unable to reach processing service")
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error in ask_summary for company '{company}': {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
