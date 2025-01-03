tp_test="training/data/tp_diff_companies_test_dataset.json"
tp_train="training/data/tp_train_dataset.json"
yt_test="training/data/yt_test_dataset.json"
yt_train="training/data/yt_train_dataset_balanced.json"
base_model="training/distilbert_base_sst2_finetuned"

# Check if the files already exist
if [ ! -f "$tp_test" ]; then 
    echo "Downloading Trustpilot test..." 
    mkdir -p training/data
    gdown https://drive.google.com/uc?id=1PeJnwpbcpeG1QTEwTHMaU8dIMz1I_BEG --output training/data//tp_diff_companies_test_dataset.json
else
    echo "Trustpilot test already exists."
fi

if [ ! -f "$tp_train" ]; then
    echo "Downloading Trustpilot train..." 
    gdown https://drive.google.com/uc?id=1z8vI1WSHba6Ag7OfzXfBEpPp8OtKrH3B --output training/data//tp_train_dataset.json
else
    echo "Trustpilot train already exists."
fi

if [ ! -f "$yt_test" ]; then 
    echo "Downloading YouTube test..."
    gdown https://drive.google.com/uc?id=17me6NiVPgogKvhDdiIfu-BC38QSsjx3b --output training/data//yt_test_dataset.json
else
    echo "YouTube test already exists."
fi

if [ ! -f "$yt_train" ]; then 
    echo "Downloading YouTube train..."
    gdown https://drive.google.com/uc?id=1ZkrkmXUJE67ieP6OlwCz8Vx9u6sKURBc --output training/data//yt_train_dataset_balanced.json
else
    echo "YouTube train already exists."
fi

if [ ! -d "$base_model" ]; then 
    echo "Downloading DistilBERT parameters..." 
    mkdir -p training/distilbert_base_sst2_finetuned
    gdown https://drive.google.com/uc?id=19raJEl3YLNpwOvGHwvNEJwhZcd2mILEJ --output training/distilbert_base_sst2_finetuned/config.json
    gdown https://drive.google.com/uc?id=1gybnajCgmz8z5rkhiMTdoSbx8wPYc6ea --output training/distilbert_base_sst2_finetuned/model.safetensors
    gdown https://drive.google.com/uc?id=13-NEVuqD9OxX6dVIdXKzp1nogI_TfGoz --output training/distilbert_base_sst2_finetuned/special_tokens_map.json
    gdown https://drive.google.com/uc?id=1AGAsNakMJMedqr9QLzZCGBlHQJ1U855e --output training/distilbert_base_sst2_finetuned/tokenizer_config.json
    gdown https://drive.google.com/uc?id=19KUefBh-84h9gT9sCH1rNYf1PPd4nM_N --output training/distilbert_base_sst2_finetuned/vocab.txt
else
    echo "DistilBERT parameters already exist."
fi

python3 training/main.py