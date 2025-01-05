import torch
from torch.utils.data import DataLoader
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
from utils import get_dataset, print_epoch_metrics
from trainer import run_epoch
import yaml
import os

def main():
    """
    Main function to evaluate a transformer-based sentiment analysis model on test data.

    Workflow:
        1. Load configuration and test dataset.
        2. Load pre-trained model and tokenizer.
        3. Evaluate the model on the train and test dataset.
        4. Output metrics (accuracy, precision, recall, etc.) in the console.
    """
    # Load configuration
    with open("training/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    # Load model and tokenizer
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = DistilBertTokenizer.from_pretrained("training/trained_model")
    model = DistilBertForSequenceClassification.from_pretrained("training/trained_model")
    model.to(device)
    
    # Load datasets
    train_dataset, test_dataset = get_dataset(
        yt_train_path=config["data"]["yt_train_path"],
        yt_test_path=config["data"]["yt_test_path"],
        tp_train_path=config["data"]["tp_train_path"],
        tp_test_path=config["data"]["tp_test_path"],
        tokenizer=tokenizer,
        tp_simple=config["data"]["tp_simple"]
    )
    train_loader = DataLoader(train_dataset, batch_size=config["training"]["batch_size"])
    test_loader = DataLoader(test_dataset, batch_size=config["training"]["batch_size"])

    loss_config = {
        "tp_loss": torch.nn.CrossEntropyLoss(label_smoothing=config["training"]["tp_label_smoothing"]),
        "yt_loss": torch.nn.CrossEntropyLoss(label_smoothing=config["training"]["yt_label_smoothing"]),
        "tp_weight": config["training"]["tp_weight"],
        "yt_weight": config["training"]["yt_weight"]
    }
    
    # Evaluate the model
    print("Evaluating on the train set...")
    train_results = run_epoch(
        model=model,
        dataloader=train_loader,
        loss_config=loss_config,
        label_names=["Positive", "Negative", "Neutral"],
        writer=None,
        device=device,
        desc="Evaluating - Training Set",
        step=0,
        prefix="train",
        optimizer=None
    )

    print("Evaluating on the test set...")
    test_results = run_epoch(
        model=model,
        dataloader=test_loader,
        loss_config=loss_config,
        label_names=["Positive", "Negative", "Neutral"],
        writer=None,
        device=device,
        desc="Evaluating - Test Set",
        step=0,
        prefix="test",
        optimizer=None 
    )

    print_epoch_metrics(epoch=0, epochs=0, train_results=train_results, val_results=test_results)

if __name__ == "__main__":
    main()