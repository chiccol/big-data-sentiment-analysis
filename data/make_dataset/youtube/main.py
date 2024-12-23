import json
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from youtube import fetch_and_store_comments
import json
import os
from dotenv import load_dotenv
import googleapiclient.discovery

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
    model : any torch like model designed for sequence classification i.e. the output must be the logits of: contradiction, neutral, entailment 
        The pretrained transformer model used for classification.
    
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
    
    scraped_comments = "youtube_comments_dataset.json"

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