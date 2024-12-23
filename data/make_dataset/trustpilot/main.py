import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json 
import os 
from time import sleep
from tqdm import tqdm

def scrape_reviews(company, from_date, date_format, stars, from_page=1, to_page=999999, language="en"):
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

def store_reviews(new_reviews, path):
    with open(path, 'r') as file:
        reviews = json.load(file)
    reviews.extend(new_reviews)
    with open(path, 'w') as file:
        json.dump(reviews, file)

def main():
    training_dataset_path = "training_dataset.json"
    out_of_company_dataset_path = "diff_companies_test_dataset.json"

    if not os.path.exists(training_dataset_path):
        with open(training_dataset_path, 'w') as file:
            json.dump([], file)
    
    if not os.path.exists(out_of_company_dataset_path):
        with open(out_of_company_dataset_path, 'w') as file:
            json.dump([], file)

    companies_from_date_path = "trustpilot.json"
    with open(companies_from_date_path, 'r') as file:
        companies_date = json.load(file)

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    for dataset in companies_date.keys():
        print(f"Building {dataset} dataset...")
        
        # Wrap the outer loop with tqdm for a progress bar
        with tqdm(companies_date[dataset].keys(), desc="Companies", unit="company", dynamic_ncols=True, leave=False) as pbar:
            for company in pbar:
                pbar.set_description(f"Processing {company}")  # Update bar description dynamically
                new_reviews = []
                
                try:
                    from_date = datetime.strptime(companies_date[dataset][company], date_format)
                except:
                    raise AssertionError(f"The date '{companies_date[dataset][company]}' does NOT match the format '{date_format}'")
                
                # Get reviews for different star ratings
                for stars in range(1, 6):
                    if stars != 3:
                        to_page = 2 if dataset == "training" else 1
                    else:
                        to_page = 4 if dataset == "training" else 2
                    
                    # Scrape reviews
                    star_new_reviews = scrape_reviews(company=company,
                                                    from_date=from_date,
                                                    stars=stars,
                                                    date_format=date_format,
                                                    to_page=to_page)
                    new_reviews.extend(star_new_reviews)
                
                # Use tqdm.write to avoid clashing with the progress bar
                tqdm.write(f"Scraped {len(new_reviews)} reviews for {company}.")
                
                if dataset == "training":
                    store_reviews(new_reviews, training_dataset_path)
                    tqdm.write(f"Added {len(new_reviews)} reviews for {company} to the training")
                elif dataset == "out_of_sample":
                    store_reviews(new_reviews, out_of_company_dataset_path)
                    tqdm.write(f"Added {len(new_reviews)} reviews for {company} to the out-of-sample")
                
                tqdm.write("*" * 50)
    
        print(f"Finished building {dataset} dataset.")

if __name__ == "__main__":
    main()