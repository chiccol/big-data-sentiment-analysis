from trainer import get_model, run_epoch
from utils import get_dataset, print_epoch_metrics
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from torch.optim import AdamW
from torch.nn import CrossEntropyLoss
import torch 
import yaml
import os 

def print_training_parameters(config, model, exp_path):
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    num_layers = config["model_params"]["trainable_transformer_layers"]
    
    print("\nTraining Parameters:")
    print("• Model name: ", config["model_params"]["hf_model"])
    print("• Total parameters: ", total_params)
    print("• Trainable parameters: ", trainable_params)
    print("• Number of trainable layers: ", num_layers)
    print("• Learning rate (lr): ", config["training"]["lr"])
    print("• Batch size: ", config["training"]["batch_size"])
    print("• Number of epochs: ", config["training"]["epochs"])
    print("• Trustpilot label smoothing: ", config["training"]["tp_label_smoothing"])
    print("• YouTube label smoothing: ", config["training"]["yt_label_smoothing"])
    print("• Trustpilot weight: ", config["training"]["tp_weight"])
    print("• YouTube weight: ", config["training"]["yt_weight"])
    print("• Train YouTube data: ", "Yes" if config["data"]["yt_train_path"] != "None" else "No")
    print("• Train Trustpilot data: ", "Yes" if config["data"]["tp_train_path"] != "None" else "No")
    print("• Only simple Trustpilot data: ", "Yes" if config["data"]["tp_simple"] else "No")
    print("• Save path: ", exp_path)

def main():

  # Load configuration
  with open("training/config.yaml", "r") as file:
    config = yaml.safe_load(file)

  use_yotube = "yes" if config["data"]["yt_train_path"] != "None" else "no"
  use_trustpilot = "yes" if config["data"]["tp_train_path"] != "None" else "no"
  param_path = (
    f"_lr_{config['training']['lr']}_wd_{config['training']['weight_decay']}_bs_{config['training']['batch_size']}"
    f"_layers_{config['model_params']['trainable_transformer_layers']}_tp_simple_{config['data']['tp_simple']}_yt_{use_yotube}_yt_{use_trustpilot}"
    f"_yt_weight{config["training"]["yt_weight"]}_tp_weight{config["training"]["tp_weight"]}_yt_smoothing{config["training"]["yt_label_smoothing"]}"
    f"_tp_weight{config["training"]["tp_label_smoothing"]}"
    )
  exp_path = os.path.join(config["experiments"]["path"], param_path)

  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  model, tokenizer = get_model(config["model_params"]["hf_model"], 
                               trainable_layers=int(config["model_params"]["trainable_transformer_layers"]))
  model.to(device)
  # Optimizer and Loss
  optimizer = AdamW(
     model.parameters(), 
     lr=float(config["training"]["lr"]),
     weight_decay=float(config["training"]["weight_decay"])
     )
  
  first_epoch = 0
  best_val_loss = float("inf")
  if os.path.exists(exp_path):
    print("Experiment already exists. Loading model and optimizer...")
    checkpoint_path = os.path.join(exp_path,"best_checkpoint.pth")
    checkpoint = torch.load(checkpoint_path)
    model.load_state_dict(checkpoint["model_state_dict"])
    optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
    first_epoch = checkpoint["epoch"]
    best_val_loss = checkpoint["best_val_loss"]
    print(f"Loaded model weights from {checkpoint_path}")
  
  writer = SummaryWriter(log_dir=exp_path)

  tp_loss_fn = CrossEntropyLoss(label_smoothing=config["training"]["tp_label_smoothing"])
  yt_loss_fn = CrossEntropyLoss(label_smoothing=config["training"]["yt_label_smoothing"])
  loss_config = {
    "tp_loss": tp_loss_fn,
    "tp_weight" : config["training"]["tp_label_smoothing"],
    "yt_loss" : yt_loss_fn,
    "yt_weight" : config["training"]["yt_label_smoothing"]
  }

  train_dataset, val_dataset = get_dataset(yt_train_path = config["data"]["yt_train_path"], 
                                           yt_test_path = config["data"]["yt_test_path"], 
                                           tp_train_path = config["data"]["tp_train_path"], 
                                           tp_test_path = config["data"]["tp_test_path"], 
                                           tokenizer = tokenizer,
                                           tp_simple=config["data"]["tp_simple"])
  
  train_loader = DataLoader(train_dataset, batch_size=config["training"]["batch_size"], shuffle=True)
  test_loader = DataLoader(val_dataset, batch_size=config["training"]["batch_size"])

  epochs = config["training"]["epochs"]
  print_training_parameters(config, model, exp_path)
  for epoch in range(epochs):
    # Run training
    train_results = run_epoch(
        model, 
        train_loader, 
        loss_config,
        optimizer=optimizer, 
        device=device, 
        desc=f"Epoch {epoch + first_epoch + 1}/{epochs + first_epoch} - Training",
        label_names=["Positive", "Negative", "Neutral"], 
        writer=writer, 
        step=epoch + first_epoch, 
        prefix="train"
    )

    # Run validation
    val_results = run_epoch(
        model,
        test_loader,
        loss_config,
        optimizer=None,
        device=device,
        desc=f"Epoch {epoch + first_epoch + 1}/{epochs + first_epoch} - Validating",
        label_names=["Positive", "Negative", "Neutral"],
        writer=writer,
        step=epoch + first_epoch,
        prefix="validation"
    )

    if val_results[0] < best_val_loss:
        best_val_loss = val_results[0]
        save_path = os.path.join(exp_path, "best_checkpoint.pth")
        torch.save({
            "epoch": epoch + 1,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "best_val_loss": best_val_loss
        }, save_path)
        print(f"New best model saved at epoch {epoch + 1} with validation loss: {best_val_loss:.4f}")

    # Print results
    print_epoch_metrics(epoch + 1, epochs, train_results, val_results)

if __name__ == "__main__":
  main()