tp_test="training/data/tp_diff_companies_test_dataset.json"
tp_train="training/data/tp_train_dataset.json"
yt_test="training/data/yt_test_dataset.json"
yt_train="training/data/yt_train_dataset_balanced.json"
trained_model_path="training/trained_model"

# Check if the files already exist
if [ ! -d "training/data" ]; then 
    mkdir -p training/data
fi

# Check if the files already exist
if [ ! -f "$tp_test" ]; then 
    echo "Downloading Trustpilot test..." 
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

if [ ! -d "$trained_model_path" ]; then 
    echo "Downloading DistilBERT weights and tokenizer..." 
    mkdir -p $trained_model_path 
    gdown --no-cookies https://drive.google.com/uc?id=193mt22Duu-qSBA16UIyzd8ILSi4v0ZUo --output training/trained_model/model.safetensors 
    gdown --no-cookies https://drive.google.com/uc?id=1o1cdVf_CYMqWqch81J0p3S-TECQGd37U --output training/trained_model/special_tokens_map.json
    gdown --no-cookies https://drive.google.com/uc?id=1ZUUpHBFJazgtU6jG1JBJtG-zTlBOvFPs --output training/trained_model/tokenizer_config.json 
    gdown --no-cookies https://drive.google.com/uc?id=1k0WCS19GRiEZAC16Jw34sA7EAJUX3lgp --output training/trained_model/vocab.txt 
    gdown --no-cookies https://drive.google.com/uc?id=1JAgsuvufUH4XLxPCXVzp2V79IqAG4l76 --output training/trained_model/config.json
else 
    echo "DistilBERT weights and tokenizer were already downloaded." 
fi

python3 training/test.py