#!/bin/bash

# Start the Spark master process in the background
spark-class org.apache.spark.deploy.master.Master &

# Run the job every 10 seconds
while true; do
    # Submit the Spark job
    spark-submit main.py
    
    # Wait 10 seconds before running the job again
    sleep 10
done