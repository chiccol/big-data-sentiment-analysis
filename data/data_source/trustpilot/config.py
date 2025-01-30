import os

CONFIG = {
    "client_id": "trustpilot-producer",
    "source": "trustpilot",
    "date_format": "%Y-%m-%dT%H:%M:%S.%fZ",
    "bootstrap_servers": os.getenv("KAFKA_ADVERTISED_LISTENERS", "kafka:9092"),
    "companies_info_path": "companies.json",
    "companies_date_path": "trustpilot_companies.json",
    "sleep_time": 30
}
