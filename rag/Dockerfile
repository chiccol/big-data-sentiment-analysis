FROM python:3.11.1-slim

WORKDIR /app

# Copy only requirements first to leverage build cache
COPY requirements.txt /app

# Install system dependencies and Python packages in a single layer
RUN apt-get update && \
    apt-get install -y gcc && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the rest of the application files
COPY rag /app

CMD ["python3", "main.py"]
