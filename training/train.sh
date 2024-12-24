tp_test="training/diff_companies_test_dataset.json"
tp_train="training/training_dataset.json"
yt_test="training/test_dataset.json"
yt_train="training/train_dataset_balanced.json"

# Check if the files already exist
if [ ! -f "$tp_test" ]; then
    echo "Downloading Trustpilot test..."
    gdown https://drive.google.com/uc?id=1wPJC00XvvTJ0wqQry0EmDG0hRmy7-pcd --output training/diff_companies_test_dataset.json
else
    echo "Trustpilot test already exists."
fi

if [ ! -f "$tp_train" ]; then
    echo "Downloading Trustpilot train..."
    gdown https://drive.google.com/uc?id=1oXOdq8x4NuqAfbaR2ak8n4atclAlHxdz --output training/training_dataset.json
else
    echo "Trustpilot train already exists."
fi

if [ ! -f "$yt_test" ]; then
    echo "Downloading YouTube test..."
    gdown https://drive.google.com/uc?id=1ij9G4y2MqRuP1-s0KEcBSwGtimWz6uyf --output training/test_dataset.json
else
    echo "YouTube test already exists."
fi

if [ ! -f "$yt_train" ]; then
    echo "Downloading YouTube train..."
    gdown https://drive.google.com/uc?id=13OCC_WsLB2Vwt3pz2nm_hEFjNszfLV15 --output training/train_dataset_balanced.json
else
    echo "YouTube train already exists."
fi

python3 training/main.py