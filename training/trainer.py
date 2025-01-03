import torch
import torch.nn as nn
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification

from utils import compute_metrics, compute_label_wise_metrics, compute_source_wise_metrics, log_metrics
from tqdm import tqdm
from typing import Dict, Optional, Tuple, List, Any
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter

def run_epoch(
    model: nn.Module,
    dataloader: DataLoader,
    loss_config: Dict[str, Any],
    label_names: List[str],
    writer: SummaryWriter,
    device: str,
    desc: str,
    step: int,
    prefix: str,
    optimizer: Optional[torch.optim.Optimizer] = None,
    ) -> Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    """
    Runs a single epoch of training or evaluation and saves all the metrics in using tensorboard.
    Args:
        model (nn.Module): The model to train or evaluate.
        dataloader (DataLoader): The data loader for training or validation data.
        loss_config (Dict[str, Any]): Configuration for loss functions and weights (e.g., `tp_loss`, `yt_loss`, etc.).
        label_names (List[str]): List of label names for calculating label-wise metrics.
        writer (SummaryWriter): TensorBoard writer for logging metrics.
        device (str): The device to run the model on ("cuda" or "cpu").
        desc (str): Description for the progress bar.
        step (int): The current step (used for TensorBoard logging).
        prefix (str): Prefix for TensorBoard metric names.
        optimizer (Optional[torch.optim.Optimizer], optional): The optimizer used for training. If `None`, the model is in evaluation mode.

    Returns:
        Tuple[float, Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
            - avg_loss (float): The average loss for the epoch.
            - metrics (Dict[str, float]): Global evaluation metrics (accuracy, precision, etc.).
            - label_metrics (Dict[str, Dict[str, float]]): Label-wise metrics (accuracy, precision, etc.).
            - source_metrics (Dict[str, Dict[str, float]]): Source-wise metrics (accuracy, precision, etc.).
    """
    is_train = optimizer is not None
    model.train() if is_train else model.eval()

    total_loss = 0.0
    y_true, y_pred, sources = [], [], []
    tp_loss = loss_config["tp_loss"]
    tp_weight = loss_config["tp_weight"]
    yt_loss = loss_config["yt_loss"]
    yt_weight = loss_config["yt_weight"]

    loop = tqdm(dataloader, desc=desc, leave=False, dynamic_ncols=True, unit="batch")
    for batch in loop:
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["label"].to(device)
        source = batch["source"]

        with torch.set_grad_enabled(is_train):
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits
            
            # Separate indices based on the source
            trustpilot_indices = [i for i, s in enumerate(source) if s == "trustpilot"]
            youtube_indices = [i for i, s in enumerate(source) if s == "youtube"]

            # Compute losses for each source
            loss_trustpilot = tp_loss(logits[trustpilot_indices], labels[trustpilot_indices]) if trustpilot_indices else 0
            loss_youtube = yt_loss(logits[youtube_indices], labels[youtube_indices]) if youtube_indices else 0

            # Combine the losses if needed (e.g., weighted sum)
            loss = loss_trustpilot * tp_weight + loss_youtube * yt_weight

            if is_train:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        total_loss += loss.item()
        preds = torch.argmax(logits, dim=1)
        batch_accuracy = (preds == labels).sum().item() / preds.shape[0]

        y_true.extend(labels.cpu().tolist())
        y_pred.extend(preds.cpu().tolist())
        sources.extend(source)

        loop.set_postfix({
            "Batch Loss": f"{loss.item():.4f}",
            "Batch Accuracy": f"{batch_accuracy:.4f}"
        })

    avg_loss = total_loss / len(dataloader)
    metrics = compute_metrics(y_true, y_pred)
    label_metrics = compute_label_wise_metrics(y_true, y_pred, label_names=label_names)
    source_metrics = compute_source_wise_metrics(y_true, y_pred, sources)

    # Log metrics and loss to TensorBoard
    if writer is not None and step is not None:
        writer.add_scalar(f"{prefix}/loss", avg_loss, step)
        tensorboard_metrics = {
            "global": metrics,
            "label_wise": label_metrics,
            "source_wise": source_metrics,
        }
        log_metrics(writer, tensorboard_metrics, step, prefix)

    return avg_loss, metrics, label_metrics, source_metrics

def get_model(
    path: str,
    trainable_layers: int
    ) -> Tuple[nn.Module, DistilBertTokenizer]:
    """
    Loads and customizes a DistilBERT model for sequence classification.
    Args:
        path (str): The path to the pre-trained DistilBERT model.
        trainable_layers (int): The number of layers to unfreeze from the pre-trained transformer.
                               All layers before these will be frozen.
    Returns:
        Tuple[nn.Module, DistilBertTokenizer]: 
            - model (nn.Module): The modified DistilBERT model with a custom classifier head.
            - tokenizer (DistilBertTokenizer): The tokenizer corresponding to the model.
    """
    # Load pre-trained DistilBERT model and tokenizer
    tokenizer = DistilBertTokenizer.from_pretrained(path)
    model = DistilBertForSequenceClassification.from_pretrained(path)
    # Change the output layer to match the number of classes (3 for this case)
    model.classifier = nn.Linear(768, 3, bias=True)
    # Freeze all layers except the classification head
    for param in model.base_model.parameters():
        param.requires_grad = False
    # Unfreeze the last `trainable_layers` layers
    if trainable_layers > 0:
        for param in model.base_model.transformer.layer[-trainable_layers:].parameters():
            param.requires_grad = True
    return model, tokenizer