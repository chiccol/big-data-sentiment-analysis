from time import sleep
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import pandas as pd

def encode_message_to_parquet(data):
    # Infer the schema from the data
    schema = pa.Table.from_pandas(pd.DataFrame(data)).schema

    # Convert the data to an Arrow Table using the inferred schema
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    # Write the table to an in-memory bytes buffer as Parquet
    buffer = BytesIO()
    pq.write_table(table, buffer)

    # Return the Parquet bytes for saving or sending
    return buffer.getvalue()

# Function to scrape Trustpilot reviews for a specific company
def scrape_and_send_reviews(company, from_date, date_format, producer, from_page=1, to_page=999999, language="en"):
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
    text = []
    language = "www" if language == "en" else language
    review_list = []
    for num_page in range(from_page, to_page + 1):
        print(f"Scraping page {num_page} for {company}...")

        if num_page > 1:
            result = requests.get(f"https://{language}.trustpilot.com/review/{company}?page={num_page}&sort=recency")
        else:
            result = requests.get(f"https://{language}.trustpilot.com/review/{company}?sort=recency")

        if result.status_code != 200:
            print(f"Error {result.status_code} while scraping page {num_page} for {company}. If Error 404, robably no more reviews available.")
            return 0
        
        soup = BeautifulSoup(result.content, 'html.parser')

        locations = soup.find_all('div', {'class': 'typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua'})
        ratings = soup.find_all('div', {'class': 'styles_reviewHeader__iU9Px'})
        dates_html = soup.find_all('div', {'class': 'styles_reviewHeader__iU9Px'})
        dates = [dates_html[i].find("time")["datetime"] for i in range(len(dates_html))]

        # Extracting titles and contents separately
        review_containers = soup.find_all('div', {'class': 'styles_reviewContent__0Q2Tg'})
        for review in review_containers:
            # First part of the review is usually the title
            title = review.find('h2').get_text() if review.find('h2') else "No Title"
            content = review.find('p').get_text() if review.find('p') else "No Content"
            review = title + " " + content
            text.append(review)

        for num_review in range(len(text)):
            full_review = dict()
            try:
                full_review["source"] = "Trustpilot"
                full_review["text"] = text[num_review]
                full_review["date"] = dates[num_review]
                full_review["tp-location"] = locations[num_review].get_text()
                full_review["tp-stars"] = int(ratings[num_review]["data-service-review-rating"])
                # check if the review is older than the specified date
                if datetime.strptime(full_review["date"],date_format) < from_date:
                    print(f"Reached reviews older than {from_date}. Stopping scraping for {company}.")
                    if num_review == 0 and num_page == 1:
                        print(f"No new reviews found for {company} after date {from_date}.")
                        return 1
                    else:
                        print(f"All reviews of {company} from date {from_date.strftime(date_format)} have been collected.")
                        return 1
                print(full_review)
                review_list.append(full_review) 
            except Exception as e:
                print(f"Error while scraping review {num_review} on page {num_page} for {company}: {e}")
                print(f"Length location: {len(locations)}, num_review: {num_review}")
        
        text.clear()
        review_list_serialized = encode_message_to_parquet(review_list)
        producer.produce(record = review_list_serialized, topic=company)
        sleep(10)   # Sleep for a short time to avoid being blocked by Trustpilot