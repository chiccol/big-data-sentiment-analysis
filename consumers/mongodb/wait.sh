#!/bin/bash

echo "Waiting 40 seconds for Kafka to be ready..."

sleep 40

echo "Starting main.py application..."
exec python3 main.py
