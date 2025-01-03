import requests
from bs4 import BeautifulSoup
from typing import List, Union, Dict
from time import sleep
from datetime import datetime
import json

def scrape_reviews(
    company: str,
    from_date: datetime,
    date_format: str,
    stars: int,
    from_page: int = 1,
    to_page: int = 999999,
    language: str = "en"
    ) -> Union[List[Dict[str, Union[str, int]]], int]:
    """
    Scrape reviews from Trustpilot for a specific company within a specified date range.
    Args:
        company (str): Trustpilot company identifier (e.g., "company.com").
        from_date (datetime): Oldest date for reviews to be collected.
        date_format (str): Format of the date used for comparison.
        stars (int): Star rating (1-5) for reviews to scrape.
        from_page (int, optional): Starting page number. Defaults to 1.
        to_page (int, optional): Ending page number. Defaults to 999999.
        language (str, optional): Language of reviews. Defaults to "en" i.e. English.
    Returns:
        List[Dict[str, Union[str, int]]]: A list of scraped reviews as dictionaries.
        int: Returns 0 if an error occurs or no reviews are found.
    """

    ratings = []
    locations = []
    dates = []
    review_list = []
    language = "www" if language == "en" else language
    url = f"https://{language}.trustpilot.com/review/{company}"
    for num_page in range(from_page, to_page + 1):
        
        text = []

        print(f"Scraping page {num_page}, with {stars} stars for {company}...")
    
        if num_page > 1:
            result = requests.get(url + f"?page={num_page}&sort=recency&stars={stars}")
        else:
            result = requests.get(url + f"?sort=recency&stars={stars}")

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
            full_review["company"] = company
            try:
                full_review["text"] = text[num_review]
                full_review["tp_stars"] = int(ratings[num_review]["data-service-review-rating"])
                if full_review["tp_stars"] > 3:
                    full_review["sentiment"] = "positive"
                elif full_review["tp_stars"] < 3:
                    full_review["sentiment"] = "negative"
                else:
                    full_review["sentiment"] = "neutral"
                full_review["date"] = dates[num_review]
                full_review["company"] = company
                # check if the review is older than the specified date
                if datetime.strptime(full_review["date"],date_format) < from_date:
                    print(f"Reached reviews older than {from_date}. Stopping scraping for {company}.")
                    print(f"Last review date: {full_review['date']}")
                    if num_review == 0 and num_page == 1:
                        print(f"No new reviews found for {company} after date {from_date}.")
                        return review_list
                    else:
                        print(f"All reviews of {company} from date {from_date.strftime(date_format)} have been collected.")
                        return review_list
                review_list.append(full_review)
            except Exception as e:
                print(f"Error while scraping review {num_review} on page {num_page} for {company}: {e}")
                print(f"Length location: {len(locations)}, num_review: {num_review}")
        
        sleep(10)   # Sleep for a short time to avoid being blocked by Trustpilot
    return review_list

def store_reviews(
        new_reviews: List[Dict[str, Union[str, int]]], 
        already_stored_path: str
        ) -> None:
    """
    Takes a list of reviews in dictionary and adds them to a JSON file.
    """
    with open(already_stored_path, 'r') as file:
        reviews = json.load(file)
    reviews.extend(new_reviews)
    with open(already_stored_path, 'w') as file:
        json.dump(reviews, file)