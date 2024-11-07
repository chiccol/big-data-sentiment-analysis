#!/bin/bash

# Wait for 30 seconds
echo "Waiting 30 seconds for Kafka to be ready..."
sleep 30

echo "Starting main.py application..."
exec python3 main.py
