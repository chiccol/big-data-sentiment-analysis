from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
import pandas as pd
from sklearn.utils import resample
from sklearn.model_selection import train_test_split

from youtube import fetch_and_store_comments
import googleapiclient.discovery

import json
import os
from dotenv import load_dotenv

def analyze_sentiment(data, candidate_labels, model, tokenizer):
    """
    Perform sentiment analysis on a dataset of text samples using a given tokenizer and model.
    
    Parameters:
    ----------
    data : list of dict
        A list of dictionaries where each dictionary represents a comment with at least a 'text' key containing the comment text.
    candidate_labels : list of str
        A list of sentiment labels (e.g., ['positive', 'neutral', 'negative']) to classify the text against.
    tokenizer : transformers.PreTrainedTokenizer
        The tokenizer to preprocess the text data for the model.
    model : any 
        A torch like model designed for sequence classification i.e. the output must be the logits of: contradiction, neutral, entailment 
    
    Returns:
    -------
    list of dict
        The updated list of dictionaries, each including a new 'sentiment' key with the predicted sentiment label.
    
    Example:
    --------
    data = [
        {'text': 'This is amazing!', 'source': 'youtube', 'date': '2024-06-05T04:50:51Z'},
        {'text': 'Not good at all.', 'source': 'youtube', 'date': '2024-06-06T04:50:51Z'}
    ]
    candidate_labels = ['positive', 'neutral', 'negative']
    updated_data = analyze_sentiment(data, candidate_labels, tokenizer, model)
    
    Output:
    [
        {'text': 'This is amazing!', 'source': 'youtube', 'date': '2024-06-05T04:50:51Z', 'sentiment': 'positive'},
        {'text': 'Not good at all.', 'source': 'youtube', 'date': '2024-06-06T04:50:51Z', 'sentiment': 'negative'}
    ]
    """
    for item in data:
        text = item['text']

        # Tokenize the text for sentiment analysis
        inputs = tokenizer(
            [text] * len(candidate_labels),  # Repeat the sentence for each label
            candidate_labels,  # Pair with labels
            truncation=True,
            padding=True,
            max_length=512,
            return_tensors="pt"
        ).to("cuda")  # Move inputs to GPU

        # Get logits and compute probabilities
        with torch.no_grad():
            logits = model(**inputs).logits
        probs = torch.nn.functional.softmax(logits, dim=-1)

        # Extract sentiment probabilities (index 2 of the logits)
        # treat the labels and text as if they were hypothesis and premise 
        entailment_scores = probs[:, 2].tolist()

        # Normalize sentiment scores to sum to 1
        normalized_scores = [score / sum(entailment_scores) for score in entailment_scores]

        # Map labels to normalized scores and determine the highest score
        label_scores = dict(zip(candidate_labels, normalized_scores))
        item['sentiment'] = max(label_scores, key=label_scores.get)  # Add sentiment to the data
    return data

def balance_dataset(data, 
                    test_size=0.2, 
                    random_state=42, 
                    train_path="yt_train_dataset_balanced.json", 
                    test_path="yt_test_dataset.json"):

    pd_data = []
    for company in data:
        for comment in data[company]:
            comment["company"] = company
            pd_data.append(comment)
    df = pd.DataFrame(pd_data)

    train_df, test_df = train_test_split(df, test_size=test_size, random_state=random_state, stratify=df[['company', 'sentiment']])

    count_df = train_df.groupby(["company", "sentiment"]).size().reset_index()
    count_df.columns = ["company", "sentiment", "count"]

    # Create a balanced training dataset by oversampling "neutral"
    balanced_data = []

    # Group by company and balance sentiments
    for company, group in train_df.groupby('company'):
        print(f"Balancing sentiments in training dataset for company: {company}")

        # Determine the target count (min comments among sentiments for this company)
        target_count = count_df.loc[(count_df.company == company) & (count_df.sentiment != "neutral"), "count"].min()
        # avoid oversampling "neutral" too much
        current_count = count_df.loc[(count_df.company == company) & (count_df.sentiment == "neutral"), "count"].values[0]
        target_count = target_count if current_count*10 > target_count else current_count*10

        company_balanced = []
        
        for sentiment in group['sentiment'].unique():
            subset = group[group['sentiment'] == sentiment]
            if sentiment == "neutral":
                # Oversample "neutral" to match the target count
                oversampled = resample(
                    subset,
                    replace=True,                # Allow resampling with replacement
                    n_samples=target_count,      # Match the target count
                    random_state=42              # For reproducibility
                )
                company_balanced.append(oversampled)
            else:
                # Append the other sentiments as-is
                company_balanced.append(subset)
        
        # Combine the balanced sentiments for this company
        balanced_data.append(pd.concat(company_balanced))

    # Combine data for all companies
    train_df_balanced = pd.concat(balanced_data).reset_index(drop=True)

    train_df_balanced.to_json(train_path, orient="records")
    print(f"Balanced training dataset saved in {train_path}")
    test_df.to_json(test_path, orient="records")
    print(f"Testing dataset saved in {test_path}")
        

if __name__ == "__main__":
    with open("youtube_companies_videos.json", "r") as file:
        company_configs = json.load(file)
    api_service_name = "youtube"
    api_version = "v3"
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "youtube.env"))
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")

    youtube_scraper = googleapiclient.discovery.build(
            api_service_name, 
            api_version, 
            developerKey=DEVELOPER_KEY
        )
    
    scraped_comments = "youtube_dataset.json"

    fetch_and_store_comments(company_configs,
                             youtube_scraper,
                             output_file=scraped_comments)
    
    # Load the zero-shot classification model and tokenizer
    zero_shot_model = AutoModelForSequenceClassification.from_pretrained('facebook/bart-large-mnli')
    zero_shot_model = zero_shot_model.to("cuda")
    tokenizer = AutoTokenizer.from_pretrained('facebook/bart-large-mnli')

    # Load the JSON file containing comments
    with open(scraped_comments, "r") as file:
        comments_data = json.load(file)

    # Candidate labels for sentiment analysis
    candidate_labels = ['positive', 'neutral', 'negative']

    # Process sentiment analysis for each company
    for company, comments in comments_data.items():
        print(f"Processing sentiment analysis for company: {company}")
        comments_data[company] = analyze_sentiment(comments, candidate_labels, zero_shot_model, tokenizer)

    # Save the updated JSON with sentiment field
    with open(scraped_comments, "w") as file:
        json.dump(comments_data, file, indent=4)

    print(f"Sentiment analysis completed. Youtube dataset saved in {scraped_comments}.")

    with open("youtube_dataset.json", "r") as f:
        dataset = json.load(f)

    balance_dataset(dataset, test_size=0.2, random_state=42)