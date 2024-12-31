# Dockerfile
FROM bitnami/spark:3.4.0

# Install Python and pip
# RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory in the container
WORKDIR /app

COPY spark/main.py /app
COPY spark/utils.py /app
COPY kafka_consumer.py /app
COPY spark/run_spark_job.sh /app
COPY spark/requirements.txt /app

# Switch to root user to do higher privilege operations
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN chmod +x /app/run_spark_job.sh

# MongoDB Spark Connector, JDBC connector and its dependencies
RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.2/mongo-spark-connector_2.12-3.0.2.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.0.5/mongodb-driver-core-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.0.5/mongodb-driver-sync-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/bson/4.0.5/bson-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
	wget https://jdbc.postgresql.org/download/postgresql-42.5.6.jar -P /opt/bitnami/spark/jars/

# Run the script to download the model and tokenizer
RUN model_path="/app/model" && \
    if [ ! -d "$model_path" ]; then \
        echo "Downloading DistilBERT weights and tokenizer..." && \
        mkdir -p /app/model && \
        gdown --no-cookies https://drive.google.com/uc?id=1huZOyETxmPQA8R_EjahEf7y1VAGHnsyb --output /app/model/model.safetensors && \
        gdown --no-cookies https://drive.google.com/uc?id=1o1cdVf_CYMqWqch81J0p3S-TECQGd37U --output /app/model/special_tokens_map.json && \
        gdown --no-cookies https://drive.google.com/uc?id=1ZUUpHBFJazgtU6jG1JBJtG-zTlBOvFPs --output /app/model/tokenizer_config.json && \
        gdown --no-cookies https://drive.google.com/uc?id=1k0WCS19GRiEZAC16Jw34sA7EAJUX3lgp --output /app/model/vocab.txt && \
        gdown --no-cookies https://drive.google.com/uc?id=1JAgsuvufUH4XLxPCXVzp2V79IqAG4l76 --output /app/model/config.json; \
    else \
        echo "DistilBERT weights and tokenizer were already downloaded."; \
    fi
    
# Switch back to regular bitnami user (principle of least privilege)
USER 1001
# I think it could be used to replace the command in the docker-compose file
# Not tested though
# ENTRYPOINT ["./run_spark_job.sh"]
