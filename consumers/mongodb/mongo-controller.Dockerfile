# Start from the official Python image
FROM python:3.11.1-slim


COPY . .

# Install the Python dependencies from the requirements.txt file
RUN pip install -r ./requirements.txt

# Expose a port (if needed; adjust this to the appropriate port number)
EXPOSE 5004

# Define the command to run the scraper
CMD ["python3", "main.py"]
