#!/bin/bash

echo "Waiting 25 seconds for Kafka to be ready..."

sleep 25

echo "Starting main.py application..."
exec python3 main.py
