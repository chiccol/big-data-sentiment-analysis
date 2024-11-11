#!/bin/bash

# Wait for 30 seconds
echo "Waiting 20 seconds for Kafka to be ready..."

sleep 20

echo "Starting main.py application..."
exec python3 main.py
