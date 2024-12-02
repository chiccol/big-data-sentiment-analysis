#!/bin/bash

echo "Pseudo Producer is waiting for 20 seconds for kafka to be ready"

sleep 20

echo "Starting main.py application..."
exec python3 main.py
