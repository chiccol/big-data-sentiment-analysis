import torch
from torch.utils.data import DataLoader, Dataset
from torch.optim import AdamW
from torch.nn import CrossEntropyLoss
import torch.nn as nn
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
from sklearn.model_selection import train_test_split

import json
import pandas as pd
from tqdm import tqdm

# Hyperparameters
batch_size = 64
learning_rate = 1e-3
epochs = 10
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

hf_model = "distilbert-base-uncased-finetuned-sst-2-english"
tokenizer = DistilBertTokenizer.from_pretrained(hf_model)
model = DistilBertForSequenceClassification.from_pretrained(hf_model)
model.classifier = nn.Linear(768, 3, bias=True)
model = model.to(device)

# Freeze all layers except the classification head
for param in model.base_model.parameters():
    param.requires_grad = False

# Custom Dataset Class
class SentimentDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_len=512):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = self.texts[idx]
        label = self.labels[idx]
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
            "label": torch.tensor(label, dtype=torch.long)
        }
    
train_path = r"/content/training_dataset.json"
test_path = r"/content/diff_companies_test_dataset.json"

train_data = pd.read_json(train_path)
test_data = pd.read_json(test_path)

def categorical_encoding(data: pd.Series):
  if data == "negative":
    return 0
  elif data == "neutral":
    return 1
  elif data == "positive":
    return 2

train_data["sentiment_encoded"] = train_data.sentiment.apply(categorical_encoding)
test_data["sentiment_encoded"] = test_data.sentiment.apply(categorical_encoding)

# Prepare Dataset
train_texts = train_data.text
train_labels = train_data.sentiment_encoded

test_texts = test_data.text
test_labels = test_data.sentiment_encoded

train_dataset = SentimentDataset(train_texts, train_labels, tokenizer)
val_dataset = SentimentDataset(test_texts, test_labels, tokenizer)

train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=batch_size)

# Optimizer and Loss
optimizer = AdamW(model.parameters(), lr=learning_rate)
loss_fn = CrossEntropyLoss()

# Training Loop
for epoch in range(epochs):
    model.train()
    train_loss = 0.0
    correct = 0
    print(f"Epoch {epoch + 1}/{epochs}")
    for batch in tqdm(train_loader, desc="Training", leave=False, ncols=80):
        optimizer.zero_grad()

        # Move data to device
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["label"].to(device)

        # Forward pass
        outputs = model(input_ids=input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        loss = loss_fn(logits, labels)

        # Backward pass
        loss.backward()
        optimizer.step()

        train_loss += loss.item()
        preds = torch.argmax(logits, dim=1)
        correct += (preds == labels).sum().item()

        # Print loss for this batch
        tqdm.write(f"Batch Loss: {loss.item():.4f}", end="\r")

    avg_train_loss = train_loss / len(train_loader)
    avg_accuracy = correct / len(train_loader.dataset)  # Correctly adjusted
    print(f"Epoch {epoch + 1}/{epochs}, Training Loss: {avg_train_loss:.4f}, Training Accuracy: {avg_accuracy:.4f}")

    # Validation Loop
    model.eval()
    val_loss = 0.0
    correct = 0
    total = 0
    for batch in tqdm(val_loader, desc="Validating", leave=False, ncols=80):
        with torch.no_grad():
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["label"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits
            loss = loss_fn(logits, labels)

            val_loss += loss.item()

            # Calculate accuracy
            preds = torch.argmax(logits, dim=1)
            correct += (preds == labels).sum().item()

    avg_val_loss = val_loss / len(val_loader)
    accuracy = correct / len(val_loader.dataset)  # Correctly adjusted
    print(f"Validation Loss: {avg_val_loss:.4f}, Accuracy: {accuracy:.4f}")