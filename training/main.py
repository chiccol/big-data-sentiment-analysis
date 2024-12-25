from trainer import get_model, run_epoch
from utils import get_dataset, print_epoch_metrics
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from torch.optim import AdamW
from torch.nn import CrossEntropyLoss
import torch 
import yaml
import os 

def main():

  # Load configuration
  with open("training/config.yaml", "r") as file:
    config = yaml.safe_load(file)

  use_yotube = "yes" if config["data"]["yt_train_path"] != "None" else "no"
  param_path = f"_lr_{config['model_params']['lr']}_bs_{config['model_params']['batch_size']}_epochs_{config['model_params']['epochs']} \
                  _layers_{config['model_params']['trainable_transformer_layers']}_tp_simple_{config['data']['tp_simple']}_yt_{use_yotube}"
  exp_path = os.path.join(config["experiments"]["path"], param_path)
  
  writer = SummaryWriter(log_dir=exp_path)

  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  model, tokenizer = get_model(config["model_params"]["hf_model"], 
                               trainable_layers=int(config["model_params"]["trainable_transformer_layers"]))
  model.to(device)
  # Optimizer and Loss
  optimizer = AdamW(model.parameters(), lr=float(config["model_params"]["lr"]))
  first_epoch = 0
  best_val_loss = float("inf")
  if config["model_params"]["weights_path"] != "None":
    checkpoint = torch.load(config["model_params"]["weights_path"])
    model.load_state_dict(checkpoint["model_state_dict"])
    optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
    first_epoch = checkpoint["epoch"]
    best_val_loss = checkpoint["best_val_loss"]
    print(f"Loaded model weights from {config['model_params']['weights_path']}")

  loss_fn = CrossEntropyLoss()

  train_dataset, val_dataset = get_dataset(yt_train_path = config["data"]["yt_train_path"], 
                                           yt_test_path = config["data"]["yt_test_path"], 
                                           tp_train_path = config["data"]["tp_train_path"], 
                                           tp_test_path = config["data"]["tp_test_path"], 
                                           tokenizer = tokenizer,
                                           tp_simple=config["data"]["tp_simple"])
  
  train_loader = DataLoader(train_dataset, batch_size=config["model_params"]["batch_size"], shuffle=True)
  test_loader = DataLoader(val_dataset, batch_size=config["model_params"]["batch_size"])

  epochs = config["model_params"]["epochs"]
  for epoch in range(epochs):
    # Run training
    train_results = run_epoch(
        model, 
        train_loader, 
        loss_fn, 
        optimizer=optimizer, 
        device=device, 
        desc=f"Epoch {epoch + 1}/{epochs} - Training",
        label_names=["Positive", "Negative", "Neutral"], 
        writer=writer, 
        step=epoch + first_epoch, 
        prefix="train"
    )

    # Run validation
    val_results = run_epoch(
        model,
        test_loader,
        loss_fn,
        optimizer=None,
        device=device,
        desc=f"Epoch {epoch + 1}/{epochs} - Validating",
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