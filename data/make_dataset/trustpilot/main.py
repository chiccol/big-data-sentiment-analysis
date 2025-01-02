from tqdm import tqdm
import json 
import os 
from datetime import datetime
from utils import scrape_reviews, store_reviews

def main():
    """
    Main function to create Trustpilot dataset for sentiment analysis.

    1. Loads configuration file with companies and dates (trustpilot.json).
    2. For each company tries to collect 480 reviews for training and 240 for testing evenly distributed across sentiment classes:
        - 1 star  (negative): 80 reviews for training and 40 for out-of-sample
        - 2 stars (negative): 80 reviews for training and 40 for out-of-sample
        - 3 stars (neutral): 160 reviews for training and 80 for out-of-sample 
        - 4 stars (positive): 80 reviews for training and 40 for out-of-sample
        - 5 stars (positive): 80 reviews for training and 40 for out-of-sample
    3. Stores the reviews in two separate JSON files: tp_train_dataset.json and tp_diff_companies_test.
    
    The configuration file contains 24 companies for training and 8 for testing. So the final dataset will contain at most
    24*480 = 11520 reviews for training and 8*240 = 1920 reviews for testing.
    """
    training_dataset_path = "tp_train_dataset.json"
    out_of_company_dataset_path = "tp_diff_companies_test_dataset.json" # test dataset with reviews from different companies
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    if not os.path.exists(training_dataset_path):
        with open(training_dataset_path, 'w') as file:
            json.dump([], file)
    
    if not os.path.exists(out_of_company_dataset_path):
        with open(out_of_company_dataset_path, 'w') as file:
            json.dump([], file)

    # Load configuration file
    companies_from_date_path = "trustpilot.json" 
    with open(companies_from_date_path, 'r') as file:
        companies_date = json.load(file)

    for dataset in companies_date.keys():
        print(f"Building {dataset} dataset...")
        save_path = training_dataset_path if dataset == "training" else out_of_company_dataset_path
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
                        to_page = 4 if dataset == "training" else 2
                    else:
                        to_page = 8 if dataset == "training" else 4
                    # Scrape reviews
                    star_new_reviews = scrape_reviews(company=company,
                                                    from_date=from_date,
                                                    stars=stars,
                                                    date_format=date_format,
                                                    to_page=to_page)
                    new_reviews.extend(star_new_reviews)
                
                # Storing collected reviews
                tqdm.write(f"Scraped {len(new_reviews)} reviews for {company}.")
                store_reviews(new_reviews, save_path)
                tqdm.write(f"Added {len(new_reviews)} reviews for {company} to the {dataset} dataset.")
                tqdm.write("*" * 50)
    
        print(f"Finished building {dataset} dataset.")

if __name__ == "__main__":
    main()