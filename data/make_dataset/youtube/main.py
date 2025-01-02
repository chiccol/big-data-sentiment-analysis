from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch

from youtube import fetch_and_store_comments
from utils import comment_classification, balance_dataset

import json
import os

def main() -> None:
    """
    Main function to create a YouTube dataset for sentiment analysis. 

    Steps:
    ------
    1. Load or fetch YouTube comments using the configuration file youtube_companies_videos.json and 
       the official YouTube API.
    2. Perform topic classification using a zero-shot classification model: "product-feedback", "video-feedback", "other-comments".
    3. Perform sentiment analysis on the classified comments: "positive", "neutral", "negative".
    4. Save the updated dataset with topics and sentiments at youtube_dataset.json.
    5. Balance the dataset for training across sentiment classes.
    6. Split the dataset into training and testing sets and stores them at yt_train_dataset_balanced.json and yt_test_dataset.json.

    Returns:
    -------
    None
    """
    with open("youtube_companies_videos.json", "r") as file:
        company_configs = json.load(file)
    
    scraped_comments = "youtube_dataset.json"

    if not os.path.exists(scraped_comments):
        fetch_and_store_comments(company_configs,
                                 output_file=scraped_comments)
    else:
        print("Using existing dataset.")
    
    # Load the zero-shot classification model and tokenizer
    zero_shot_model_path = "facebook/bart-large-mnli" # Replace with more powerful models to improve the final results
    zero_shot_model = AutoModelForSequenceClassification.from_pretrained(zero_shot_model_path)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    zero_shot_model = zero_shot_model.to(device)
    tokenizer = AutoTokenizer.from_pretrained(zero_shot_model_path)

    # Load the JSON file containing comments
    with open(scraped_comments, "r") as file:
        comments_data = json.load(file)

    # Candidate labels for topic classification
    candidate_labels = ["product-feedback", "video-feedback", "other-comments"]

    # Process topic analysis for each company
    for company, comments in comments_data.items():
        print(f"Processing topic analysis for company: {company}")
        comments_data[company] = comment_classification(comments, 
                                                        candidate_labels, 
                                                        zero_shot_model, 
                                                        tokenizer, 
                                                        device, 
                                                        "topic", 
                                                        threshold = 0.45)

    # Candidate labels for sentiment analysis
    candidate_labels = ['positive', 'neutral', 'negative']

    # Process sentiment analysis for each company
    for company, comments in comments_data.items():
        print(f"Processing sentiment analysis for company: {company}")
        comments_data[company] = comment_classification(comments, 
                                                        candidate_labels, 
                                                        zero_shot_model, 
                                                        tokenizer, 
                                                        device, 
                                                        "sentiment", 
                                                        threshold = 0)

    # Save the updated JSON with sentiment and topic field
    with open(scraped_comments, "w") as file:
        json.dump(comments_data, file, indent=4)

    print(f"Sentiment analysis completed. Youtube dataset saved in {scraped_comments}.")

    with open("youtube_dataset.json", "r") as f:
        dataset = json.load(f)

    balance_dataset(dataset, 
                    test_size=0.2, 
                    random_state=42, 
                    train_path = "yt_train_dataset_balanced.json", 
                    test_path = "yt_test_dataset.json")  

if __name__ == "__main__":
    main()