tp_test="training/tp_diff_companies_test_dataset.json"
tp_train="training/tp_training_dataset.json"
yt_test="training/yt_test_dataset.json"
yt_train="training/yt_train_dataset_balanced.json"

# Check if the files already exist
if [ ! -f "$tp_test" ]; then 
    echo "Downloading Trustpilot test..." 
    gdown https://drive.google.com/uc?id=1EguvQcsht2zbHhxt-XqXWZ81fgVuHVxu --output training/diff_companies_test_dataset.json
else
    echo "Trustpilot test already exists."
fi

if [ ! -f "$tp_train" ]; then
    echo "Downloading Trustpilot train..." 
    gdown https://drive.google.com/uc?id=14Z2nbEyqOTkU2FD4EkWxUzUTaWs_CuBQ --output training/tp_training_dataset.json
else
    echo "Trustpilot train already exists."
fi

if [ ! -f "$yt_test" ]; then 
    echo "Downloading YouTube test..."
    gdown https://drive.google.com/uc?id=1WqV01XrxhEwERehjdmBXvJTJQjGWLPMX --output training/yt_test_dataset.json
else
    echo "YouTube test already exists."
fi

if [ ! -f "$yt_train" ]; then 
    echo "Downloading YouTube train..."
    gdown https://drive.google.com/uc?id=12EwKMCeEdljQG4SQ0of4FqZ1SnobIdbk --output training/yt_train_dataset_balanced.json
else
    echo "YouTube train already exists."
fi

python3 training/main.py