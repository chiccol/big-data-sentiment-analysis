#!/bin/bash

echo "Reddit producer is waiting 10 seconds for Kafka to be ready..."

sleep 10

echo "Starting main.py application..."
exec python3 main.py
