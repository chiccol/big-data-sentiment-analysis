from time import sleep
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json

# Helper function to scrape data and append to a list
def soup2list(src, list_, attr=None):
    if attr:
        for val in src:
            list_.append(val[attr])
    else:
        for val in src:
            list_.append(val.get_text())

# Function to scrape Trustpilot reviews for a specific company
def scrape_reviews(company, from_page=1, to_page=2):
    users = []
    ratings = []
    locations = []
    dates = []
    reviews = []
    review_json = []

    for i in range(from_page, to_page + 1):
        print(f"Scraping page {i} for {company}...")

        # Construct the URL for the current company and page
        result = requests.get(f"https://www.trustpilot.com/review/{company}?page={i}")
        soup = BeautifulSoup(result.content, 'html.parser')

        # Scrape the relevant data
        soup2list(soup.find_all('span', {'class': 'typography_heading-xxs__QKBS8 typography_appearance-default__AAY17'}), users)
        soup2list(soup.find_all('div', {'class': 'typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua'}), locations)
        soup2list(soup.find_all('div', {'class': 'styles_reviewHeader__iU9Px'}), ratings, attr='data-service-review-rating')

        dates_html = soup.find_all('div', {'class': 'styles_reviewHeader__iU9Px'})
        dates = [dates_html[i].find("time")["datetime"] for i in range(len(dates_html))]

        # Extracting titles and contents separately
        review_containers = soup.find_all('div', {'class': 'styles_reviewContent__0Q2Tg'})
        for review in review_containers:
            # First part of the review is usually the title
            title = review.find('h2').get_text() if review.find('h2') else "No Title"
            content = review.find('p').get_text() if review.find('p') else "No Content"
            review = title + " " + content
            reviews.append(review)

        sleep(5)

    # Convert the scraped data into a DataFrame
    review_data = pd.DataFrame({
        'Username': users,
        'Location': locations,
        'Date': dates,
        'Review': reviews,  # Add contents separately
        'Rating': ratings
    })

    # Return the DataFrame containing the reviews
    return review_data

# Main function to read company URLs from file and save each to a JSON file
def scrape_and_save_reviews():
    with open("urls-trustpilot.txt", 'r') as file:
        companies = [line.strip() for line in file.readlines()]  # Read and strip newlines

    for company in companies:
        print(f"Scraping reviews for {company}...")

        # Scrape reviews for the company
        review_data = scrape_reviews(company)

        # Save the scraped data to a JSON file
        json_filename = f"{company.replace('.', '_')}_reviews.json"
        review_data.to_json(json_filename, orient="records", indent=4)

        print(f"Data saved to {json_filename}")

# Run the scraping and saving process
if __name__ == "__main__":
    scrape_and_save_reviews()