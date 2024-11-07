# Start from the official Python image
FROM python:3.11.1-slim

# Install bash and Kafka tools
RUN apt-get update && \
    apt-get install -y wget gnupg bash && \
    wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz && \
    tar xzf kafka_2.13-3.5.1.tgz && \
    mv kafka_2.13-3.5.1/bin/* /usr/local/bin/ && \
    rm -rf kafka_2.13-3.5.1 kafka_2.13-3.5.1.tgz && \
    apt-get remove -y wget gnupg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the Python files into the container
COPY mongodb /app

# Install the Python dependencies
RUN pip install -r ./requirements.txt

# Copy and set permissions for the kafka check script
COPY mongodb/check-kafka.sh /app/
RUN chmod +x /app/check-kafka.sh

# Explicitly specify bash for the script
ENTRYPOINT ["/bin/bash", "/app/check-kafka.sh"]
