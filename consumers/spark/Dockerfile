# Dockerfile
FROM bitnami/spark:3.4.0

# Install Python and pip
# RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory in the container
WORKDIR /app

# Copy the folder containing the scraper's Python files into the container
COPY spark/main.py /app
COPY spark/requirements.txt /app
COPY spark/model /app/model
COPY spark/utils.py /app
COPY kafka_consumer.py /app

COPY spark/run_spark_job.sh /app
USER root
RUN chmod +x /app/run_spark_job.sh

# Install transformers and torch
RUN pip install --no-cache-dir -r requirements.txt

# Install wget to download the connectors 
RUN apt-get update && apt-get install -y wget

# MongoDB Spark Connector, JDBC connector and its dependencies
RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.2/mongo-spark-connector_2.12-3.0.2.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.0.5/mongodb-driver-core-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.0.5/mongodb-driver-sync-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/mongodb/bson/4.0.5/bson-4.0.5.jar -P /opt/bitnami/spark/jars/ && \
	wget https://jdbc.postgresql.org/download/postgresql-42.5.6.jar -P /opt/bitnami/spark/jars/

# I think it could be used to replace the command in the docker-compose file
# Not tested though
# ENTRYPOINT ["./run_spark_job.sh"]
