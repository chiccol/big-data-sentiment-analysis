from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import numpy as np 
import pandas as pd
from torch.utils.data import Dataset
import torch 

from collections import defaultdict

# Custom Dataset Class
class SentimentDataset(Dataset):
    def __init__(self, texts, labels, source, tokenizer, max_len=512):
        self.texts = texts
        self.labels = labels
        self.source = source
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = self.texts[idx]
        label = self.labels[idx]
        source = self.source[idx]
        encoding = self.tokenizer(
            text,
            truncation=True,
            padding="max_length",
            max_length=self.max_len,
            return_tensors="pt"
        )
        return {
            "input_ids": encoding["input_ids"].squeeze(0),
            "attention_mask": encoding["attention_mask"].squeeze(0),
            "label": torch.tensor(label, dtype=torch.long),
            "source" : source
        }

def get_dataset(yt_train_path, yt_test_path, tp_train_path, tp_test_path, tokenizer, tp_simple=True):

  print("Loading Trustpilot data...")
  tp_train_data = pd.read_json(tp_train_path)
  tp_train_data["source"] = "trustpilot"
  tp_test_data = pd.read_json(tp_test_path)
  tp_test_data["source"] = "trustpilot"
  if tp_simple:
    tp_train_data = tp_train_data.loc[~tp_train_data.tp_stars.isin([2,4]),:]
    tp_test_data = tp_test_data.loc[~tp_test_data.tp_stars.isin([2,4]),:]

  if yt_test_path == "None" or yt_train_path == "None": 
    print("Ignoring Youtube data")
    yt_test_data = pd.DataFrame(columns = tp_train_data.columns)
    yt_train_data = pd.DataFrame(columns = tp_train_data.columns) 
  else:
    print("Loading Youtube data...")
    yt_test_data = pd.read_json(yt_test_path)
    yt_test_data["source"] = "youtube"
    yt_train_data = pd.read_json(yt_train_path)
    yt_train_data["source"] = "youtube"


  train_data = pd.concat([tp_train_data.loc[:,["text","sentiment", "source"]],
                        yt_train_data.loc[:,["text","sentiment", "source"]]])
  test_data = pd.concat([tp_test_data.loc[:,["text","sentiment", "source"]],
                        yt_test_data.loc[:,["text","sentiment", "source"]]])
  
  sentiment_mapping = {"negative":0, "neutral":1, "positive":2}
  train_data["sentiment_encoded"] = train_data.sentiment.map(sentiment_mapping)
  test_data["sentiment_encoded"] = test_data.sentiment.map(sentiment_mapping)

  # Prepare Dataset
  train_texts = train_data.text.to_list()
  train_labels = train_data.sentiment_encoded.to_list()
  train_source = train_data.source.to_list()

  test_texts = test_data.text.to_list()
  test_labels = test_data.sentiment_encoded.to_list()
  test_source = test_data.source.to_list()

  train_dataset = SentimentDataset(train_texts, train_labels, train_source, tokenizer)
  test_dataset = SentimentDataset(test_texts, test_labels, test_source, tokenizer)

  return train_dataset, test_dataset

def compute_metrics(y_true, y_pred, round_digits=4):
    accuracy = round(accuracy_score(y_true, y_pred), round_digits)
    precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="macro", zero_division=0)
    return {
        "accuracy": accuracy,
        "precision": round(precision, round_digits),
        "recall": round(recall, round_digits),
        "f1": round(f1, round_digits),
    }

def compute_label_wise_metrics(y_true, y_pred, label_names, round_digits=4):
    precision, recall, f1, support = precision_recall_fscore_support(y_true, y_pred, average=None, zero_division=0)
    
    metrics = {}
    for i, label in enumerate(label_names):
        # Compute accuracy for this label
        correct = np.sum((np.array(y_true) == i) & (np.array(y_pred) == i))  # Correct predictions for this label
        total = support[i]  # Total occurrences of the label in the true labels
        label_accuracy = correct / total if total > 0 else 0  # Accuracy for the label

        # Store the label-wise metrics
        metrics[label] = {
            "accuracy": round(label_accuracy, round_digits),
            "precision": round(precision[i], round_digits),
            "recall": round(recall[i], round_digits),
            "f1": round(f1[i], round_digits),
        }
    
    return metrics

def compute_source_wise_metrics(y_true, y_pred, sources, round_digits=4):
    source_metrics = defaultdict(lambda: defaultdict(float))
    source_to_labels = defaultdict(list)
    source_to_preds = defaultdict(list)

    for src, label, pred in zip(sources, y_true, y_pred):
        source_to_labels[src].append(label)
        source_to_preds[src].append(pred)

    for src in source_to_labels:
        metrics = compute_metrics(source_to_labels[src], source_to_preds[src])
        source_metrics[src] = {k: round(v, round_digits) for k, v in metrics.items()}

    return dict(source_metrics)

def print_epoch_metrics(epoch, epochs, train_results, val_results):
    print(f"\nEpoch {epoch}/{epochs}")

    train_loss, train_global, train_labels, train_sources = train_results
    print(f"\nTraining Metrics:")
    print(f"  Loss: {train_loss:.4f}")
    print(f"  Global: {train_global}")
    print(f"  Label-wise:")
    for label, metrics in train_labels.items():
        print(f"    {label}: {metrics}")
    print(f"  Source-wise:")
    for source, metrics in train_sources.items():
        print(f"    {source}: {metrics}")

    val_loss, val_global, val_labels, val_sources = val_results
    print(f"\nValidation Metrics:")
    print(f"  Loss: {val_loss}")
    print(f"  Global: {val_global}")
    print(f"  Label-wise:")
    for label, metrics in val_labels.items():
        print(f"    {label}: {metrics}")
    print(f"  Source-wise:")
    for source, metrics in val_sources.items():
        print(f"    {source}: {metrics}")
    print("-" * 50)