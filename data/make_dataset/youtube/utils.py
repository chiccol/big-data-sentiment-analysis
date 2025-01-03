from transformers import PreTrainedModel, PreTrainedTokenizer
import torch
import pandas as pd
from sklearn.utils import resample
from sklearn.model_selection import train_test_split

from typing import List, Dict, Any

def comment_classification(
    data: List[Dict[str, str]], 
    candidate_labels: List[str], 
    model: PreTrainedModel, 
    tokenizer: PreTrainedTokenizer, 
    device: str, 
    classification: str, 
    threshold: float = 0.45
    ) -> List[Dict[str, str]]:
    """
    Classify comments into specific categories or sentiments using a sequence classification model.

    Parameters:
    ----------
    data : list of dict
        A list of dictionaries where each dictionary represents a comment. Each dictionary should 
        have a 'text' key containing the comment text, and optionally other metadata like 'topic'.
    candidate_labels : list of str
        A list of labels (e.g., ['positive', 'neutral', 'negative'] or other custom labels) to classify 
        the text against.
    tokenizer : transformers.PreTrainedTokenizer
        The tokenizer used to preprocess the text data for the model. It should be compatible with 
        the given model.
    model : transformers.PreTrainedModel
        A sequence classification model that outputs logits corresponding to 
        [contradiction, neutral, entailment] for input text-label pairs.
    device: str
        The device to run the model on (e.g., 'cuda' for GPU or 'cpu' for CPU).
    classification : str
        The type of classification to perform (e.g., 'sentiment', 'topic', etc.). This determines 
        how specific scenarios are handled (e.g., hard-coded rules for specific topics).
    threshold : float, optional (default=0.45)
        The minimum normalized entailment score required to assign a label. If no label exceeds 
        this threshold, the label is set to 'unknown'.

    Returns:
    -------
    list of dict
        The updated list of dictionaries. Each dictionary will include a new key (based on the 
        classification type, e.g., 'sentiment') with the predicted label.

    Notes:
    ------
    - If a comment's 'topic' is 'unknown', the 'sentiment' classification is directly set to 'unknown'.
    - If the 'topic' is classified as 'other-comments' or 'video-feedback', the 'sentiment' classification is 
      hard-coded to 'neutral'.
    - The text and candidate labels are treated as premise and hypothesis pairs for the model.
    - Normalized entailment scores are used to assign the best label based on the highest score 
      exceeding the threshold.

    Example:
    --------
    data = [
        {'text': 'This product is amazing!', 'source': 'youtube', 'topic': 'review-product'},
        {'text': 'The video quality is great.', 'source': 'youtube', 'topic': 'video-feedback'},
        {'text': 'Not relevant at all.', 'source': 'youtube', 'topic': 'unknown'}
    ]
    candidate_labels = ['positive', 'neutral', 'negative']
    
    updated_data = comment_classification(data, candidate_labels, model, tokenizer, classification='sentiment')

    Output:
    [
        {'text': 'This product is amazing!', 'source': 'youtube', 'topic': 'review-product', 'sentiment': 'positive'},
        {'text': 'The video quality is great.', 'source': 'youtube', 'topic': 'video-feedback', 'sentiment': 'neutral'},
        {'text': 'Not relevant at all.', 'source': 'youtube', 'topic': 'unknown', 'sentiment': 'unknown'}
    ]
    """
    for item in data:
        if item.get("sentiment", None): continue
        text = item['text']

        # hard-coding sentiment
        if classification == "sentiment":
          if item["topic"] == "unknown":
            item[classification] = "unknown"
            continue
          elif item["topic"] in ("other-comments", "video-feedback"):
            item[classification] = "neutral"
            continue

        # Tokenize the text for sentiment analysis
        inputs = tokenizer(
            [text] * len(candidate_labels),  # Repeat the sentence for each label
            candidate_labels,  # Pair with labels
            truncation=True,
            padding=True,
            max_length=512,
            return_tensors="pt"
        ).to(device)  # Move inputs to GPU

        # Get logits and compute probabilities
        with torch.no_grad():
            logits = model(**inputs).logits
        probs = torch.nn.functional.softmax(logits, dim=-1)

        # Extract sentiment probabilities (index 2 of the logits)
        # treat the labels and text as if they were hypothesis and premise
        entailment_scores = probs[:, 2].tolist()

        # Normalize sentiment scores to sum to 1
        normalized_scores = [score / sum(entailment_scores) for score in entailment_scores]

        if max(normalized_scores) > threshold: 
          # Map labels to normalized scores and determine the highest score
          label_scores = dict(zip(candidate_labels, normalized_scores))
          item[classification] = max(label_scores, key=label_scores.get)  # Add sentiment to the data
        else:
          # If the topic classification is too uncertain (below threshold), set topic to "unknown"
          item[classification] = "unknown"

    return data

def balance_dataset(
    data: Dict[str, List[Dict[str, Any]]], 
    test_size: float = 0.2, 
    random_state: int = 42, 
    train_path: str = "yt_train_dataset_balanced.json", 
    test_path: str = "yt_test_dataset.json"
    ) -> None:
    """
    Balance a dataset by undersampling and oversampling sentiments for each company.
    Parameters:
    ----------
    data : dict
        A dictionary where keys are company names and values are lists of comment dictionaries. 
        Each comment dictionary should have a 'sentiment' key (e.g., 'positive', 'neutral', 'negative').
    test_size : float, optional (default=0.2)
        The proportion of the dataset to include in the test split.
    random_state : int, optional (default=42)
        Random seed for reproducibility.
    train_path : str, optional (default="yt_train_dataset_balanced.json")
        Path to save the balanced training dataset as a JSON file.
    test_path : str, optional (default="yt_test_dataset.json")
        Path to save the test dataset as a JSON file.
    Returns:
    -------
    None
        The function saves the balanced training and test datasets to the specified paths.
    Notes:
    ------
    - Removes comments with 'unknown' sentiment before splitting.
    - Balances the training dataset by undersampling 'neutral' comments to match the count of 
      the most frequent non-neutral sentiment for each company.
    - The test dataset is stratified by company and sentiment to ensure proportional representation.
    """
    pd_data = []
    for company in data:
        for comment in data[company]:
            comment["company"] = company
            pd_data.append(comment)
    df = pd.DataFrame(pd_data)
    df = df.loc[df.sentiment != "unknown"]
    
    train_df, test_df = train_test_split(df, test_size=test_size, random_state=random_state, stratify=df[['company', 'sentiment']])

    count_df = train_df.groupby(["company", "sentiment"]).size().reset_index()
    count_df.columns = ["company", "sentiment", "count"]

    # Create a balanced training dataset by undersampling "neutral"
    balanced_data = []

    # Group by company and balance sentiments
    for company, group in train_df.groupby('company'):
        print(f"Balancing sentiments in training dataset for company: {company}")

        # Determine the target count (max comments among sentiments for this company)
        target_count = count_df.loc[(count_df.company == company) & (count_df.sentiment != "neutral"), "count"].max()
        current_count = count_df.loc[(count_df.company == company) & (count_df.sentiment == "neutral"), "count"].values[0]
        # Sample with replacement if count(neutral) < count(max(positive,negative))
        replace = True if current_count < target_count else False
        company_balanced = []

        for sentiment in group['sentiment'].unique():
            subset = group[group['sentiment'] == sentiment]
            if sentiment == "neutral":
                # Undersample "neutral" to match the target count
                oversampled = resample(
                    subset,
                    replace=replace,             # If True, resampling with replacement
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