import os

CONFIG = {
    "client_id": "trustpilot-producer",
    "source": "trustpilot",
    "date_format": "%Y-%m-%dT%H:%M:%S.%fZ",
    "bootstrap_servers": os.getenv("KAFKA_ADVERTISED_LISTENERS", "kafka:9092"),
    "companies_date_path": "urls-trustpilot.json",
    "sleep_time": 30
}
