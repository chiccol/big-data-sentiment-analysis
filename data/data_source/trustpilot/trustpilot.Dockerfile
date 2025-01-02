FROM python:3.11.1-slim

WORKDIR /app

# Copy only requirements first to leverage build cache
COPY trustpilot/requirements.txt .

# Install system dependencies and Python packages in a single layer
RUN apt-get update && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the rest of the application files
COPY trustpilot /app
COPY kafka_producer.py /app

# Set executable permissions in one layer
RUN chmod +x wait.sh

# Set entrypoint and default command
ENTRYPOINT ["/app/wait.sh"]
CMD ["python3", "main.py"]
