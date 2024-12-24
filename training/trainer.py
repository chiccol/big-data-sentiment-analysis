import torch
import torch.nn as nn
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification

from utils import compute_metrics, compute_label_wise_metrics, compute_source_wise_metrics
from tqdm import tqdm

def run_epoch(
    model, 
    dataloader, 
    loss_fn, 
    optimizer=None, 
    device="cuda", 
    desc="Training", 
    label_names=None
):
    is_train = optimizer is not None
    model.train() if is_train else model.eval()

    total_loss = 0.0
    correct = 0
    y_true, y_pred, sources = [], [], []

    loop = tqdm(dataloader, desc=desc, leave=False, ncols=80, unit="batch")
    for batch in loop:
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["label"].to(device)
        source = batch["source"]

        with torch.set_grad_enabled(is_train):
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits
            loss = loss_fn(logits, labels)
            if is_train:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        total_loss += loss.item()
        preds = torch.argmax(logits, dim=1)
        correct += (preds == labels).sum().item()

        y_true.extend(labels.cpu().tolist())
        y_pred.extend(preds.cpu().tolist())
        sources.extend(source)

        loop.set_postfix({"Batch Loss": f"{loss.item():.4f}"})

    avg_loss = total_loss / len(dataloader)
    metrics = compute_metrics(y_true, y_pred)
    label_metrics = compute_label_wise_metrics(y_true, y_pred, label_names=label_names)
    source_metrics = compute_source_wise_metrics(y_true, y_pred, sources)
    return avg_loss, metrics, label_metrics, source_metrics

def get_model(path):
  tokenizer = DistilBertTokenizer.from_pretrained(path)
  model = DistilBertForSequenceClassification.from_pretrained(path)
  model.classifier = nn.Linear(768, 3, bias=True)
  # Freeze all layers except the classification head
  for param in model.base_model.parameters():
      param.requires_grad = False
  return model, tokenizer