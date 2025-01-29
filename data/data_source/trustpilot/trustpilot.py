import requests
from bs4 import BeautifulSoup

import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import json
import pandas as pd

import logging
from typing import List, Dict
from datetime import datetime
from time import sleep

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Output to console
        # Optionally add file logging
        # logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger("trustpilot-producer")
logger.info("Started logging")

def encode_message_to_parquet(data: List[Dict[str, str]]) -> bytes:
    """
    Encodes a list of dictionaries into an in-memory Parquet table.
    Args:
        data (List[Dict[str, str]]): The data to encode as a list of dictionaries.
    Returns:
        bytes: The Parquet-encoded data as a byte array.
    """
    # Infer the schema from the data
    schema = pa.Table.from_pandas(pd.DataFrame(data)).schema

    # Convert the data to an Arrow Table using the inferred schema
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    # Write the table to an in-memory bytes buffer as Parquet
    buffer = BytesIO()
    pq.write_table(table, buffer)
    logger.info(f"Encoded the message successfully")
    # Return the Parquet bytes for saving or sending
    return buffer.getvalue()

def scrape_and_send_reviews(
    company:str,
    company_for_scraping: str,
    from_date: datetime,
    date_format: str,
    producer,
    from_page: int = 1,
    to_page: int = 999999,
    language: str = "en"
    ) -> int:
    """
    Scrape reviews from Trustpilot for a specific company within a specified date range.

    This function retrieves reviews from Trustpilot's website for a given company, collecting user information,
    ratings, locations, review dates, and review text. It continues scraping until it reaches either the specified 
    number of pages or reviews older than a specified date. 
    Produces a list of byte encoded json. 
    Args:
        company (str): The Trustpilot company identifier for which reviews are being scraped in the format company.com.
        from_date (datetime): The minimum date for reviews to be collected. Reviews older than this date will 
                              stop the scraping process.
        date_format (str): The format in which the date of the last time scraping was performed is provided, used for 
                           date comparison.
        from_page (int, optional): The starting page number for scraping. Defaults to 1.
        to_page (int, optional): The ending page number for scraping. Defaults to 999999 because main interest in scraping 
                                 by date, not by page.
        language (str, optional): The language in which the reviews should be scraped. Defaults to "en". 
                                  If "en", it is converted to "www" for the URL.
    Returns:
        int: Returns 0 if an error occurs (e.g., a 404 error), 1 if scraping completes successfully, 
             or returns 1 if no new reviews are found after the specified date.
    """

    ratings = []
    locations = []
    dates = []
    num_reviews = 0
    language = "www" if language == "en" else language
    url = f"https://{language}.trustpilot.com/review/{company_for_scraping}"
    for num_page in range(from_page, to_page + 1):
        reviews_list = []
        logger.info(f"Scraping page {num_page} for {company}...")
        if num_page > 1:
            result = requests.get(url + f"?page={num_page}&sort=recency")
        else:
            result = requests.get(url + "?sort=recency")
        if result.status_code != 200:
            logger.error(f"Error {result.status_code} while scraping page {num_page} for {company}. If Error 404, robably no more reviews available.")
            return 0
        
        soup = BeautifulSoup(result.content, 'html.parser')

        dict_reviews = soup.find_all("script")[-1].text
        dict_reviews = json.loads(dict_reviews)["props"]["pageProps"]["reviews"]
        
        for num_review,review in enumerate(dict_reviews):
            full_review = dict()
            # Accept only reviews with a date and rating
            try:
                full_review["date"] = review["dates"]["publishedDate"]
                if datetime.strptime(full_review["date"],date_format) < from_date:
                    logger.info(f"Reached reviews older than {from_date}. Stopping scraping for {company}.") 
                    if num_review == 0 and num_page == 1:
                        logger.info(f"No new reviews found for {company} after date {from_date}.")
                        return 1
                    else:
                        logger.info(f"All reviews of {company} from date {from_date.strftime(date_format)} have been collected.")   
                        num_reviews += len(reviews_list)
                        logger.info(f"Scraped {num_reviews} reviews for {company} so far.")
                        review_list_serialized = encode_message_to_parquet(reviews_list)
                        producer.produce(record = review_list_serialized, topic=company)
                        return 1
                full_review["stars"] = int(review.get("rating"))
            except Exception as e:
                error_message = "\n".join([f"Error while scraping review {num_review} on page {num_page} for {company}: {e}\n",
                        f"Length location: {len(locations)}, num_review: {num_review}"])
                logger.error(error_message)
            # Accept reviews without text because we have the rating (-> sentiment)
            title = review.get("title", "")
            text = review.get("text", "")
            full_review["text"] = title + " " + text

            full_review["tp_location"] = review["consumer"].get("countryCode", "N/A") if "consumer" in review else "N/A"
            full_review["source"] = "trustpilot"
            full_review["company"] = company
            
            reviews_list.append(full_review)

        num_reviews += len(reviews_list)
        logger.info(f"Scraped {num_reviews} reviews for {company} so far.")
        review_list_serialized = encode_message_to_parquet(reviews_list)
        reviews_list.clear()
        producer.produce(record = review_list_serialized, topic=company)
        sleep(10)   # Sleep for a short time to avoid being blocked by Trustpilot
