# Use the official Python 3.11 image as the base image
FROM python:3.11

# Copy all files from the current directory into the container's working directory
COPY . .

# Install the required Python packages from requirements.txt
RUN pip install -r ./requirements.txt 

# Expose port 5000 for the application (used for web traffic)
EXPOSE 5000

# Set the command to run the application when the container starts
CMD ["python","main.py"]
