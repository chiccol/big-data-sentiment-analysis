#!/bin/bash
# Maximum number of retries
MAX_RETRIES=30
# Delay between retries in seconds
DELAY=5
# Kafka broker address (assuming it's running in Docker network)
KAFKA_BROKER="kafka:9092"
# Expected topics - adjust these to match your required topics
REQUIRED_TOPICS=("nvidia" "apple")


echo "Starting Kafka readiness check..."

# Function to check if Kafka is ready
check_kafka() {
    if kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to check if all required topics exist
check_topics() {
    local existing_topics
    existing_topics=$(kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list)
    
    for topic in "${REQUIRED_TOPICS[@]}"; do
        if ! echo "$existing_topics" | grep -q "^$topic$"; then
            echo "Topic $topic not found"
            return 1
        fi
    done
    return 0
}

# Main loop
counter=0
while [ $counter -lt $MAX_RETRIES ]; do
    echo "Attempt $((counter+1)) of $MAX_RETRIES"
    
    if check_kafka; then
        echo "Kafka is up, checking topics..."
        if check_topics; then
            echo "All required topics are present!"
            echo "Starting main.py application..."
            # Start the main application
            exec python3 main.py
            # The exec command replaces the current process, so we won't reach any code after this
            exit 0
        else
            echo "Not all required topics are ready yet"
        fi
    else
        echo "Kafka is not ready yet"
    fi
    
    counter=$((counter+1))
    
    if [ $counter -eq $MAX_RETRIES ]; then
        echo "Maximum retries reached. Kafka or topics not ready. Exiting."
        exit 1
    fi
    
    echo "Waiting $DELAY seconds before next attempt..."
    sleep $DELAY
done
