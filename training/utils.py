from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import numpy as np 
import pandas as pd
from torch.utils.data import Dataset
import torch 

from transformers import PreTrainedTokenizerBase
from typing import List, Union, Tuple, Dict, Any
from collections import defaultdict

class SentimentDataset(Dataset):
    """
    A custom dataset for sentiment analysis that in addition to returns the training
    data, also returns the source of the data which is useful for source-wise evaluation. 

    Attributes:
        texts (List[str]): List of input texts.
        labels (List[Union[int, float]]): Corresponding sentiment labels for each text.
        source (List[str]): Additional source metadata for each text.
        tokenizer (PreTrainedTokenizerBase): Tokenizer instance to process the texts.
        max_len (int): Maximum length for tokenized sequences. Defaults to 512.
    """

    def __init__(
        self,
        texts: List[str],
        labels: List[Union[int, float]],
        source: List[str],
        tokenizer: PreTrainedTokenizerBase,
        max_len: int = 512,
    ):
        """
        Initializes the SentimentDataset.
        Args:
            texts (List[str]): List of input texts.
            labels (List[Union[int, float]]): Corresponding sentiment labels for each text.
            source (List[str]): Additional source (youtube, trustpilot etc...) metadata for each text.
            tokenizer (PreTrainedTokenizerBase): Tokenizer instance to process the texts.
            max_len (int, optional): Maximum length for tokenized sequences. Defaults to 512.
        """
        self.texts = texts
        self.labels = labels
        self.source = source
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self) -> int:
        """
        Returns the number of samples in the dataset.
        Returns:
            int: Length of the dataset.
        """
        return len(self.texts)

    def __getitem__(self, idx: int) -> dict:
        """
        Retrieves a single data sample as a dictionary.
        Args:
            idx (int): Index of the sample to retrieve.
        Returns:
            dict: A dictionary containing the input IDs, attention mask, label, and 
                  source.
        """
        text = self.texts[idx]
        label = self.labels[idx]
        source = self.source[idx]

        # Tokenize the text with truncation and padding
        encoding = self.tokenizer(
            text,
            truncation=True,
            padding="max_length",
            max_length=self.max_len,
            return_tensors="pt",
        )

        return {
            "input_ids": encoding["input_ids"].squeeze(0),
            "attention_mask": encoding["attention_mask"].squeeze(0),
            "label": torch.tensor(label, dtype=torch.long),
            "source": source,
        }

def get_dataset(
    yt_train_path: str,
    yt_test_path: str,
    tp_train_path: str,
    tp_test_path: str,
    tokenizer: PreTrainedTokenizerBase,
    tp_simple: bool = True,
) -> Tuple[SentimentDataset, SentimentDataset]:
    """
    Loads and prepares datasets for sentiment analysis from YouTube and Trustpilot sources.

    Args:
        yt_train_path (str): Path to the YouTube training dataset json file.
        yt_test_path (str): Path to the YouTube test dataset json file.
        tp_train_path (str): Path to the Trustpilot training dataset json file.
        tp_test_path (str): Path to the Trustpilot test dataset json file.
        tokenizer (PreTrainedTokenizerBase): Tokenizer to preprocess text data.
        tp_simple (bool, optional): Whether to simplify Trustpilot data by excluding certain ratings 
                                    with 2 and 4 stars which are harder to classify. Defaults to True.

    Returns:
        Tuple[SentimentDataset, SentimentDataset]: A tuple containing the training and test datasets.
    """
    # If Trustpilot data is not provided, create empty DataFrames
    if tp_train_path == "None" or tp_test_path == "None":
        print("Ignoring Trustpilot data")
        tp_train_data = pd.DataFrame(columns=["text", "sentiment", "source"])
        tp_test_data = pd.DataFrame(columns=["text", "sentiment", "source"])
    else:
        print("Loading Trustpilot data...")
        tp_train_data = pd.read_json(tp_train_path)
        tp_train_data["source"] = "trustpilot"
        tp_test_data = pd.read_json(tp_test_path)
        tp_test_data["source"] = "trustpilot"

        if tp_simple:
            # Exclude ratings of 2 and 4 stars to make the task easier to fit 
            tp_train_data = tp_train_data.loc[~tp_train_data.tp_stars.isin([2, 4]), :]
            tp_test_data = tp_test_data.loc[~tp_test_data.tp_stars.isin([2, 4]), :]

    # If YouTube data is not provided, create empty DataFrames
    if yt_test_path == "None" or yt_train_path == "None":
        print("Ignoring YouTube data")
        yt_train_data = pd.DataFrame(columns=["text", "sentiment", "source"])
        yt_test_data = pd.DataFrame(columns=["text", "sentiment", "source"])
    else:
        print("Loading YouTube data...")
        yt_train_data = pd.read_json(yt_train_path)
        yt_train_data["source"] = "youtube"
        yt_test_data = pd.read_json(yt_test_path)
        yt_test_data["source"] = "youtube"

    # Combine datasets
    train_data = pd.concat([
        tp_train_data.loc[:, ["text", "sentiment", "source"]],
        yt_train_data.loc[:, ["text", "sentiment", "source"]],
    ])
    test_data = pd.concat([
        tp_test_data.loc[:, ["text", "sentiment", "source"]],
        yt_test_data.loc[:, ["text", "sentiment", "source"]],
    ])

    # Map sentiment labels to numeric values
    sentiment_mapping = {"negative": 0, "neutral": 1, "positive": 2}
    train_data["sentiment_encoded"] = train_data.sentiment.map(sentiment_mapping)
    test_data["sentiment_encoded"] = test_data.sentiment.map(sentiment_mapping)

    # Prepare datasets
    train_texts = train_data.text.to_list()
    train_labels = train_data.sentiment_encoded.to_list()
    train_source = train_data.source.to_list()

    test_texts = test_data.text.to_list()
    test_labels = test_data.sentiment_encoded.to_list()
    test_source = test_data.source.to_list()

    train_dataset = SentimentDataset(train_texts, train_labels, train_source, tokenizer)
    test_dataset = SentimentDataset(test_texts, test_labels, test_source, tokenizer)

    return train_dataset, test_dataset

def compute_metrics(
    y_true: List[int],
    y_pred: List[int],
    round_digits: int = 4
) -> Dict[str, float]:
    """
    Computes evaluation metrics for classification tasks.
    Args:
        y_true (List[int]): The true labels.
        y_pred (List[int]): The predicted labels.
        round_digits (int, optional): Number of decimal places to round the metrics to. Defaults to 4.
    Returns:
        Dict[str, float]: A dictionary containing the following metrics:
            - "accuracy": Classification accuracy.
            - "precision": Macro-averaged precision.
            - "recall": Macro-averaged recall.
            - "f1": Macro-averaged F1 score.
    """
    accuracy = round(accuracy_score(y_true, y_pred), round_digits)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="macro", 
        zero_division=0 # Ignore division by zero errors
    )
    return {
        "accuracy": accuracy,
        "precision": round(precision, round_digits),
        "recall": round(recall, round_digits),
        "f1": round(f1, round_digits),
    }

def compute_label_wise_metrics(
    y_true: List[int],
    y_pred: List[int],
    label_names: List[str],
    round_digits: int = 4
) -> Dict[str, Dict[str, float]]:
    """
    Computes label-wise (negative, neutral and positive) metrics for classification tasks.
    Args:
        y_true (List[int]): The true labels.
        y_pred (List[int]): The predicted labels.
        label_names (List[str]): List of label names corresponding to the indices in `y_true` and `y_pred`.
        round_digits (int, optional): Number of decimal places to round the metrics to. Defaults to 4.
    Returns:
        Dict[str, Dict[str, float]]: A dictionary where each label is a key, and the value is another dictionary 
                                     containing:
                                     - "accuracy": Accuracy for the label.
                                     - "precision": Precision for the label.
                                     - "recall": Recall for the label.
                                     - "f1": F1 score for the label.
    """
    precision, recall, f1, support = precision_recall_fscore_support(
        y_true, y_pred, average=None, zero_division=0
    )

    metrics = {}
    for i, label in enumerate(label_names):
        # Calculate accuracy for the label
        correct = np.sum((np.array(y_true) == i) & (np.array(y_pred) == i))
        total = support[i] # Total occurrences of the label in the true labels
        label_accuracy = correct / total if total > 0 else 0 # Compute accuracy safely

        # Store metrics for the label
        metrics[label] = {
            "accuracy": round(label_accuracy, round_digits),
            "precision": round(precision[i], round_digits),
            "recall": round(recall[i], round_digits),
            "f1": round(f1[i], round_digits),
        }

    return metrics


def compute_source_wise_metrics(
    y_true: List[int],
    y_pred: List[int],
    sources: List[str],
    round_digits: int = 4
) -> Dict[str, Dict[str, float]]:
    """
    Computes source-wise metrics for classification tasks.

    Args:
        y_true (List[int]): The true labels.
        y_pred (List[int]): The predicted labels.
        sources (List[str]): List of source names corresponding to each label in `y_true` and `y_pred`.
        round_digits (int, optional): Number of decimal places to round the metrics to. Defaults to 4.

    Returns:
        Dict[str, Dict[str, float]]: A dictionary where each source is a key, and the value is another dictionary 
                                     containing:
                                     - "accuracy": Accuracy for the source.
                                     - "precision": Precision for the source.
                                     - "recall": Recall for the source.
                                     - "f1": F1 score for the source.
    """
    source_metrics = defaultdict(lambda: defaultdict(float))
    source_to_labels = defaultdict(list)
    source_to_preds = defaultdict(list)

    # Group labels and predictions by source
    for src, label, pred in zip(sources, y_true, y_pred):
        source_to_labels[src].append(label)
        source_to_preds[src].append(pred)

    # Compute metrics for each source
    for src in source_to_labels:
        metrics = compute_metrics(source_to_labels[src], source_to_preds[src], round_digits=round_digits)
        source_metrics[src] = metrics

    return dict(source_metrics)

def print_epoch_metrics(
    epoch: int,
    epochs: int,
    train_results: Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]],
    val_results: Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]
) -> None:
    """
    Prints the metrics for a given epoch during training and validation.
    Args:
        epoch (int): The current epoch number.
        epochs (int): The total number of epochs.
        train_results (Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]):
            - Training loss (float).
            - Global training metrics (Dict[str, float]).
            - Label-wise training metrics (Dict[str, Dict[str, float]]).
            - Source-wise training metrics (Dict[str, Dict[str, float]]).
        val_results (Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]):
            - Validation loss (float).
            - Global validation metrics (Dict[str, float]).
            - Label-wise validation metrics (Dict[str, Dict[str, float]]).
            - Source-wise validation metrics (Dict[str, Dict[str, float]]).
    """
    print(f"\nEpoch {epoch}/{epochs}")

    # Unpack training results
    train_loss, train_global, train_labels, train_sources = train_results
    print(f"\nTraining Metrics:")
    print(f"  Loss: {train_loss:.4f}")
    print(f"  Global Metrics:")
    for metric, value in train_global.items():
        print(f"    {metric.capitalize()}: {value:.4f}")
    print(f"  Label-wise Metrics:")
    for label, metrics in train_labels.items():
        print(f"    {label}: {metrics}")
    print(f"  Source-wise Metrics:")
    for source, metrics in train_sources.items():
        print(f"    {source}: {metrics}")

    # Unpack validation results
    val_loss, val_global, val_labels, val_sources = val_results
    print(f"\nValidation Metrics:")
    print(f"  Loss: {val_loss:.4f}")
    print(f"  Global Metrics:")
    for metric, value in val_global.items():
        print(f"    {metric.capitalize()}: {value:.4f}")
    print(f"  Label-wise Metrics:")
    for label, metrics in val_labels.items():
        print(f"    {label}: {metrics}")
    print(f"  Source-wise Metrics:")
    for source, metrics in val_sources.items():
        print(f"    {source}: {metrics}")

    print("-" * 50)

def log_metrics(
        writer: torch.utils.tensorboard.SummaryWriter, 
        metrics: Dict[str, Dict[str, Any]],
        step:int, 
        prefix: str=""):
    """
    Log global, label-wise, and source-wise metrics to TensorBoard.
    Args:
        writer: TensorBoard SummaryWriter instance.
        metrics: Metrics dictionary (global, label-wise, source-wise).
        step: Current epoch or batch step.
        prefix: Prefix for metrics (e.g., 'train', 'val').
    """
    # Log global metrics
    for metric, value in metrics["global"].items():
        writer.add_scalar(f"{prefix}/global/{metric}", value, step)

    # Log label-wise metrics
    for label, label_metrics in metrics["label_wise"].items():
        for metric, value in label_metrics.items():
            writer.add_scalar(f"{prefix}/label_wise/{label}/{metric}", value, step)

    # Log source-wise metrics
    for source, source_metrics in metrics["source_wise"].items():
        for metric, value in source_metrics.items():
            writer.add_scalar(f"{prefix}/source_wise/{source}/{metric}", value, step)

def print_training_parameters(
    config: Dict[str, Dict[str, Any]], 
    model: torch.nn.Module, 
    exp_path: str
) -> None:
    """
    Prints the training parameters and configuration details.

    Args:
        config (Dict[str, Dict[str, Any]]): The configuration dictionary used for training.
        model (nn.Module): The PyTorch model instance.
        exp_path (str): The path where the experiment results are saved.
    """
    # Calculate total and trainable parameters
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    num_layers = config["model_params"]["trainable_transformer_layers"]

    print("\nTraining Parameters:")
    print(f"• Model name: {config['model_params']['hf_model']}")
    print(f"• Total parameters: {total_params:,}")
    print(f"• Trainable parameters: {trainable_params:,}")
    print(f"• Number of trainable layers: {num_layers}")
    print(f"• Learning rate (lr): {config['training']['lr']}")
    print(f"• Batch size: {config['training']['batch_size']}")
    print(f"• Number of epochs: {config['training']['epochs']}")
    print(f"• Trustpilot label smoothing: {config['training']['tp_label_smoothing']}")
    print(f"• YouTube label smoothing: {config['training']['yt_label_smoothing']}")
    print(f"• Trustpilot weight: {config['training']['tp_weight']}")
    print(f"• YouTube weight: {config['training']['yt_weight']}")
    print(f"• Train YouTube data: {'Yes' if config['data']['yt_train_path'] != 'None' else 'No'}")
    print(f"• Train Trustpilot data: {'Yes' if config['data']['tp_train_path'] != 'None' else 'No'}")
    print(f"• Only simple Trustpilot data: {'Yes' if config['data']['tp_simple'] else 'No'}")
    print(f"• Save path: {exp_path}")