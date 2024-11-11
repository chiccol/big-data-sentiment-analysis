# Start from the official Python slim image
FROM python:3.11.1-slim

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the scraper's Python files into the container
COPY mongodb /app

# Install the Python dependencies from the requirements.txt file
RUN pip install -r ./requirements.txt

# Expose a port (if needed; adjust this to the appropriate port number)
EXPOSE 5004

# Install bash
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

# Make the wait.sh script executable
RUN chmod +x /app/wait.sh

# Set wait.sh as the entrypoint
ENTRYPOINT ["/app/wait.sh"]

CMD ["python3", "main.py"]
