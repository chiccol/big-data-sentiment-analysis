# Dockerfile
FROM bitnami/spark:latest

# Install Python and pip
# RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the scraper's Python files into the container
COPY spark/main.py /app
COPY spark/requirements.txt /app
COPY spark/model /app/model
COPY kafka_consumer.py /app

COPY spark/run_spark_job.sh /app
USER root
RUN chmod +x /app/run_spark_job.sh

# Install transformers and torch
RUN pip install --no-cache-dir -r requirements.txt

# I think it could be used to replace the command in the docker-compose file
# Not tested though
# ENTRYPOINT ["./run_spark_job.sh"]