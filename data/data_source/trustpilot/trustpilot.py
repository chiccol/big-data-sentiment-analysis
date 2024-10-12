from time import sleep
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

# Function to scrape Trustpilot reviews for a specific company
def scrape_reviews(company, from_date, date_format, from_page=1, to_page=999999, language="en"):
    
    users = []
    ratings = []
    locations = []
    dates = []
    text = []
    
    language = "www" if language == "en" else language

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

        users = soup.find_all('span', {'class': 'typography_heading-xxs__QKBS8 typography_appearance-default__AAY17'})
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

        for num_review in range(len(users)):
            full_review = dict()
            full_review["Username"] = users[num_review].get_text()
            full_review["Location"] = locations[num_review].get_text()
            full_review["Rating"] = ratings[num_review]["data-service-review-rating"]
            full_review["Date"] = dates[num_review]
            if datetime.strptime(full_review["Date"],date_format) < from_date:
                print(f"Reached reviews older than {from_date}. Stopping scraping for {company}.")
                if num_review == 0 and num_page == 1:
                    print(f"No new reviews found for {company} after date {from_date}.")
                return 1
            full_review["Review"] = text[num_review]
            full_review_serialized = json.dumps(full_review).encode('utf-8')
            print(full_review_serialized)
        
        sleep(5)

    print(f"All reviews of {company} from date {from_date_str} have been collected.")
    return 1

# Run the scraping and saving process
if __name__ == "__main__":
    
    companies_from_date_path = "urls-trustpilot.json"
    with open(companies_from_date_path, 'r') as file:
        company_date = json.load(file)
        companies, from_dates_str = list(company_date.keys()), list(company_date.values())

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                    
    while True:
        for company, from_date_str in zip(companies,from_dates_str):
            
            try:
                from_date = datetime.strptime(from_date_str, date_format)
            except:
                raise AssertionError(f"The date '{from_date_str}' does NOT match the format '{date_format}'")
            
            scrape_reviews(company=company, 
                           from_date = from_date,
                           date_format = date_format,
                           language="en")
            
            # update 
            company_date[company] = datetime.now().strftime(date_format)
            with open(companies_from_date_path, 'w') as file:
                json.dump(company_date, file)

        sleep(30)  