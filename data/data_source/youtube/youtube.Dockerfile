FROM python:3.11.1-slim

WORKDIR /app

# Copy only requirements first to leverage build cache
COPY youtube/requirements.txt .

# Install system dependencies and Python packages in a single layer
RUN apt-get update && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the rest of the application files
COPY youtube/config.py /app
COPY youtube/main.py /app
COPY youtube/youtube.py /app
COPY youtube/wait.sh /app 
COPY youtube/youtube.env /app 
COPY kafka_producer.py /app
COPY companies.json /app

# Set executable permissions in one layer
RUN chmod +x wait.sh

# Set entrypoint and default command
ENTRYPOINT ["/app/wait.sh"]
CMD ["python3", "main.py"]
