# big-data-sentiment-analysis

## Table of Contents

- [How it Works](#How-it-Works)
- [How to Run](#How-to-Run)
    - [Nvidia Runtime (Using GPU)](#Nvidia-Runtime-(Using-Gpu))
    - [Using CPU](#Using-CPU)
    - [Youtube and Reddit API Keys](#Youtube-and-Reddit-Api-Keys)
        - [How to Get the Keys](#How-to-Get-the-Keys)
- [Reproducibility](#reproducibility)
    - [Training and Testing](#Training-and-Testing)
    - [Create Dataset](#Create-Dataset)

## How it Works 

This application will scrape data from the internet, run the data through a machine learning model and store the relevant data in various databases. 
The data is obtained through the official APIs of Youtube and Reddit, a custom-made web scraper for TrustPilot and a random data generator used to test heavier data loads, since every other data producer is limited in throughput (daily rates). \
More details can be found in the ```report.pdf```. \
**[INSERT ARCHITECTURE CHART]**

## How to run
Ensure you have docker installed, if you don't please follow this [guide](https://docs.docker.com/get-started/get-docker/).

```
git clone https://github.com/giuseppecurci/big-data-sentiment-analysis/
cd big-data-sentiment-analysis
```

### Nvidia Runtime (Using GPU)
If you want to use the application with a CPU then skip to the following section. \
If you you have apt installed then run the following to install the NVIDIA Container Toolkit packages (needed to use Nvidia drivers with docker):
```
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
```
If you don't have apt installed then please follow this [guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#prerequisites)

Then restart docker:
```
sudo systemctl restart docker
```
**CAVEAT**⚠️: we assume that the user already has the Nvidia drivers installed on the machine. 
To check that everything works run:
```
sudo dokcer run --rm --runtime=nvidia --gpus all nvidia/cuda:11.6.2-base-ubuntu20.04 nvidia-smi
```
You should see the nvidia-smi interface which looks like the following:
```
Wed Jan  1 17:23:54 2025       
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 550.120                Driver Version: 550.120        CUDA Version: 12.4     |
|-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  NVIDIA GeForce RTX 3060 ...    Off |   00000000:01:00.0  On |                  N/A |
| N/A   49C    P8             15W /   80W |      50MiB /   6144MiB |     13%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
                                                                                         
+-----------------------------------------------------------------------------------------+
| Processes:                                                                              |
|  GPU   GI   CI        PID   Type   Process name                              GPU Memory |
|        ID   ID                                                               Usage      |
|=========================================================================================|
|    0   N/A  N/A      2871      G   /usr/lib/xorg/Xorg                             45MiB |
+-----------------------------------------------------------------------------------------+
```
Finally run the following to start the application:
```
sudo docker compose up
```
**CAVEAT**⚠️: by default spark defines a standard batch size equal to 64 (```docker-compose.yml```, spark-master environment line 82). If you encounter CUDA out-of-memory errors you have to reduce it. 

### Using CPU

The ```docker-compose.yml``` by default uses Nvidia runtime. To use only the CPU comment the runtime and deploy options of the spark-master and spark-worker-1 i.e. lines 86-91 and 114-119. Once you have modified the file you can run:
```
sudo docker compose up 
```
**CAVEAT**⚠️: by default spark defines a standard batch size equal to 64 (```docker-compose.yml```, spark-master environment line 82). If you encounter out-of-memory errors you have to reduce it.

### Youtube and Reddit API Keys

The scrapers for Youtube and Reddit use official APIs. \
For Youtube you have to create a ```youtube.env``` file containing the API keys as follows (at least one key):
```
DEVELOPER_KEY = "YourKey"
DEVELOPER_KEY_2 = "YourKey2"
DEVELOPER_KEY_K = "YourKeyK"
```
and store the file at:
```
data/data_source/youtube
data/make_dataset/youtube
```
For Reddit create ```reddit.env``` file 
```
REDDIT_CLIENT_ID = "YourClientID"
REDDIT_CLIENT_SECRET = "YourKey"
REDDIT_USERNAME = "YourUsername"
REDDIT_USER_AGENT = "YourUserAgent"
```
and store the file at 
```
data/data_source/reddit
```
#### How to Get the Keys

- **Any User**: Follow these guides for [youtube](https://www.youtube.com/watch?v=EPeDTRNKAVo) and [reddit](INSERT A VALID URL)  
- **Big Data Technologies (UNITN Course)**: We remind you that we sent you our credentials via email along with the report. If, for any reason, you can't find the above-mentioned files please readily contact us using any of the emails we provided or through any other available communication channel.

## Reproducibility 

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
**Note**: The file ```training/config.yaml``` controls the training parameters and by default sets the parameters the ones used to obtain the best checkpoint. The corresponding tensorboard is stored at:
```
final_experiments/_lr_1e-4_wd_5e-1_bs_64_layers_3_tp_simple_True_yt_yes_yt_yes_yt_weight0.5_tp_weight1_yt_smoothing0.2_tp_weight0.1
```
To visualize all the training diagnostics run:
```
tensorboard --logdir training
```
If you are interested only in replicating the results run (**TO BE ADDED**):
```
chmod +x training/test.sh 
./training/test.sh
```

### Create Dataset

If you want to create a similar dataset to the one we used then create a separate venv for each source and run the respective scripts: 
- **Youtube** :
```
python -m venv yt_dataset_venv
source yt_dataset_venv/bin/activate
pip install -r data/make_dataset/youtube/requirements.txt
cd data/make_dataset/youtube
python3 main.py
```
- **Trustpilot**:
```
python -m venv tp_dataset_venv
source tp_dataset_venv/bin/activate
pip install -r data/make_dataset/trustpilot/requirements.txt
cd data/make_dataset/trustpilot
python3 main.py
```

The scripts create the following files in the respective directories:
- ```tp_train_dataset.json```: trustpilot training data
- ```tp_diff_companies_test_dataset.json```: trustpilot test data (from different companies to the train set)
- ```youtube_dataset.json```: the full dataset obtained after scraping and noisy classification with a zero-shot model; columns:
    - topic: whether the comment is about the product, the video or other topics. If the model is too uncertain about the topic then by default it is classified as other comments to filter out the noise and excluded from the final training.
    - sentiment: negative, neutral and positive. If the comment is about the video, then the sentiment label is hard-coded to neutral.
    - text: the scraped comment
    - company: the reference company for which the data was scraped
- ```yt_train_dataset_balanced.json```: youtube training data balanced across classification labels. TL;DR many comments in ```youtube_dataset.json``` are neutral, we account for this imbalance using downsampling to avoid a source imbalance
- ```yt_test_dataset.json```: youtube test data