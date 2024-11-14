#!/bin/bash

echo "Trustpilot Producer is waiting for 10 seconds for kafka to be ready"

sleep 10

echo "Starting main.py application..."
exec python3 main.py
