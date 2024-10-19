# Start from the official Python image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the scraper's Python files into the container
COPY youtube /app
COPY kafka_producer.py /app

# Install the Python dependencies from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the scraper
CMD ["python3", "main.py"]