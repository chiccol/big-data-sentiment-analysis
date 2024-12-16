# tests/test_main.py

import os
import pytest
from fastapi.testclient import TestClient
from main import app
from unittest.mock import MagicMock

client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def setup_env():
    # Load environment variables for testing
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "backend/backend.env"))

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the FastAPI prototype!"}

def test_get_mongo_data():
    response = client.get("/mongo-data")
    assert response.status_code == 200
    assert "mongo_data" in response.json()
    assert isinstance(response.json()["mongo_data"], list)
    # Further assertions can be added based on expected data structure

def test_get_postgres_data():
    response = client.get("/postgres-data")
    assert response.status_code == 200
    assert "postgres_data" in response.json()
    assert isinstance(response.json()["postgres_data"], list)
    # Further assertions can be added based on expected data structure

# Example of testing error handling by mocking database connections
def test_mongo_data_failure(monkeypatch):
    from database import mongo_db

    def mock_find(*args, **kwargs):
        raise Exception("MongoDB connection error")

    monkeypatch.setattr(mongo_db, "find", mock_find)
    response = client.get("/mongo-data")
    assert response.status_code == 500
    assert response.json() == {"detail": "MongoDB connection error"}

def test_postgres_data_failure(monkeypatch):
    from database import pg_connection

    def mock_cursor(*args, **kwargs):
        raise Exception("PostgreSQL query error")

    monkeypatch.setattr(pg_connection, "cursor", mock_cursor)
    response = client.get("/postgres-data")
    assert response.status_code == 500
    assert response.json() == {"detail": "PostgreSQL query error"}



def test_get_mongo_data_mocked(monkeypatch):
    from database import mongo_db

    class MockCollection:
        def find(self, *args, **kwargs):
            return [
                {
                    "source": "TestSource",
                    "text": "Test text",
                    "date": "2024-01-01T00:00:00Z",
                    "yt_video_id": "testid",
                    "yt_likes": 100,
                    "yt_reply_count": 10,
                    "tp_stars": 5,
                    "tp_location": "Test Location",
                    "re_vote": 50,
                    "re_reply_count": 5
                }
            ]

    mock_collection = MockCollection()
    monkeypatch.setattr(mongo_db, "users", mock_collection)

    response = client.get("/mongo-data")
    assert response.status_code == 200
    assert response.json() == {
        "mongo_data": [
            {
                "source": "TestSource",
                "text": "Test text",
                "date": "2024-01-01T00:00:00Z",
                "yt_video_id": "testid",
                "yt_likes": 100,
                "yt_reply_count": 10,
                "tp_stars": 5,
                "tp_location": "Test Location",
                "re_vote": 50,
                "re_reply_count": 5
            }
        ]
    }
