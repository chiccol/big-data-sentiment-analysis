# big-data-sentiment-analysis
## How to run
Ensure you have docker installed, if you don't please follow this [guide](https://docs.docker.com/get-started/get-docker/).

```
git clone https://github.com/giuseppecurci/big-data-sentiment-analysis/
cd big-data-sentiment-analysis
sudo docker compose up --build
```

## How it works

This application will scrape data from the internet, run the data through a machine learning model and store the relevant data in various databases. 
The data is obtained through the official APIs of Youtube and Reddit, a custom-made web scraper for TrustPilot and a random data generator used to test heavier data loads, since every other data producer is limited in throughput (daily rates).

### Training and Testing

First, let's create a venv with all the requirements needed: 
```
python -m venv training_venv
source training_venv/bin/activate
pip install -r training/requirements.txt
```
Then the following script will download the datasets, train the model and store the weights as well as the diagnostics:
```
chmod +x training/train.sh
./training/train.sh
```