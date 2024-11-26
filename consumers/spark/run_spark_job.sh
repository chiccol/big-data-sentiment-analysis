#!/bin/bash

# Wait for kafka and consumers to start
echo "Waiting 30 seconds for Kafka and consumers to start..."
sleep 30
# Start the Spark master process in the background
spark-class org.apache.spark.deploy.master.Master &

# Run the job every 10 seconds
while true; do
    # Submit the Spark job
    spark-submit main.py
    
    # Wait 10 seconds before running the job again
    sleep 10
done
