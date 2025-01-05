from trainer import get_model, run_epoch
from utils import get_dataset, print_epoch_metrics, print_training_parameters
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from torch.optim import AdamW
from torch.nn import CrossEntropyLoss
import torch 
import yaml
import os 

def main():
  """
  Main function to train a transformer-based sentiment analysis model using YouTube and Trustpilot data.

  Worfklow:
    1. Loads the configuration file for training and data parameters.
    2. Creates an experiment directory based on training parameters.
    3. Initializes the model, tokenizer, optimizer, and loss functions.
    4. Loads pre-existing checkpoints if available, including model weights, optimizer state, and validation metrics.
    5. Prepares training and validation datasets and their corresponding dataloaders.
    6. Runs training and validation loops for the specified number of epochs, tracking performance using a TensorBoard SummaryWriter.
    7. Saves the model checkpoint with the best validation performance.

  Configuration:
    The function expects a YAML configuration file at `training/config.yaml`.

  Outputs:
    - Training logs written to TensorBoard.
    - Checkpoint of the model saved with the lowest validation loss.
    - Console output of training and validation metrics per epoch.
  """
  # Load configuration
  with open("training/config.yaml", "r") as file:
    config = yaml.safe_load(file)

  # Create experiment directory
  use_yotube = "yes" if config["data"]["yt_train_path"] != "None" else "no"
  use_trustpilot = "yes" if config["data"]["tp_train_path"] != "None" else "no"
  param_path = (
    f"_lr_{config['training']['lr']}_wd_{config['training']['weight_decay']}_bs_{config['training']['batch_size']}"
    f"_layers_{config['model_params']['trainable_transformer_layers']}_tp_simple_{config['data']['tp_simple']}_yt_{use_yotube}_tp_{use_trustpilot}"
    f"_yt_weight{config["training"]["yt_weight"]}_tp_weight{config["training"]["tp_weight"]}_yt_smoothing{config["training"]["yt_label_smoothing"]}"
    f"_tp_weight{config["training"]["tp_label_smoothing"]}"
    )
  exp_path = os.path.join(config["experiments"]["path"], param_path)

  # Load model and tokenizer
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
  # If experiment already exists, load model, optimizer, epoch of last best checkpoint and corresponding validation loss
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

  # Source-wise loss
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
  total_epochs = epochs + first_epoch
  print_training_parameters(config, model, exp_path)
  for epoch in range(epochs):
    # Run training
    actual_epoch = epoch + first_epoch  
    train_results = run_epoch(
        model, 
        train_loader, 
        loss_config,
        optimizer=optimizer, 
        device=device, 
        desc=f"Epoch {actual_epoch + 1}/{total_epochs} - Training",
        label_names=["Positive", "Negative", "Neutral"], 
        writer=writer, 
        step=actual_epoch, 
        prefix="train"
    )

    # Run validation
    val_results = run_epoch(
        model,
        test_loader,
        loss_config,
        optimizer=None, # No optimization
        device=device,
        desc=f"Epoch {actual_epoch + 1}/{total_epochs} - Validating",
        label_names=["Positive", "Negative", "Neutral"],
        writer=writer,
        step=actual_epoch,
        prefix="validation"
    )

    # Update best model if the validation loss is better
    if val_results[0] < best_val_loss:
        best_val_loss = val_results[0]
        save_path = os.path.join(exp_path, "best_checkpoint.pth")
        torch.save({
            "epoch": actual_epoch + 1,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "best_val_loss": best_val_loss
        }, save_path)
        print(f"New best model saved at epoch {actual_epoch + 1} with validation loss: {best_val_loss:.4f}")

    # Print results
    print_epoch_metrics(actual_epoch + 1, total_epochs, train_results, val_results)

if __name__ == "__main__":
  main()