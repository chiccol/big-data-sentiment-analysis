# Start from the official Python image
FROM python:3.11.1-slim

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the scraper's Python files into the container
COPY trustpilot /app
COPY kafka_producer.py /app

# Install the Python dependencies from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt

# Expose a port (if needed; adjust this to the appropriate port number)
EXPOSE 5001

# Install bash
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Make the wait.sh script executable
RUN chmod +x /app/wait.sh

# Set wait.sh as the entrypoint
ENTRYPOINT ["/app/wait.sh"]
# Define the command to run the scraper
CMD ["python3", "main.py"]
