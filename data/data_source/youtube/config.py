import os

CONFIG = {
    "client_id": "youtube-producer",
    "date_format": "%Y-%m-%dT%H:%M:%SZ",
    "bootstrap_servers": os.getenv("KAFKA_ADVERTISED_LISTENERS", "kafka:9092"),
    "companies_videos_path": "youtube_companies_videos.json",
    "sleep_time": 600,
    "api_service_name" : "youtube",
    "api_version" : "v3"
}