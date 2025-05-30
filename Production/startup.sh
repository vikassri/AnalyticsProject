#!/bin/bash
# startup.sh

set -e

echo "Starting E-commerce Kafka Pipeline..."

# Validate environment variables
required_vars=(
    "CONFLUENT_BOOTSTRAP_SERVERS"
    "CONFLUENT_API_KEY" 
    "CONFLUENT_API_SECRET"
    "SCHEMA_REGISTRY_URL"
    "SCHEMA_REGISTRY_KEY"
    "SCHEMA_REGISTRY_SECRET"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set"
        exit 1
    fi
done

echo "✓ Environment variables validated"

# Run health check
echo "Running health checks..."
python health_check.py
if [ $? -ne 0 ]; then
    echo "Health check failed. Exiting."
    exit 1
fi

echo "✓ Health check passed"

# Create topics if they don't exist
echo "Creating topics..."
python create_topics.py

echo "✓ Topics created/verified"

# Register schemas
echo "Registering schemas..."
python register_schemas.py

echo "✓ Schemas registered"

# Start the specified service
case "$1" in
    "producer")
        echo "Starting data producers..."
        python customer_events_producer.py &
        python order_events_producer.py &
        wait
        ;;
    "recommendation")
        echo "Starting recommendation engine..."
        python recommendation_engine.py
        ;;
    "fraud-detection")
        echo "Starting fraud detection..."
        python fraud_detection.py
        ;;
    "all")
        echo "Starting all services..."
        python customer_events_producer.py &
        python order_events_producer.py &
        python recommendation_engine.py &
        python fraud_detection.py &
        wait
        ;;
    *)
        echo "Usage: $0 {producer|recommendation|fraud-detection|all}"
        exit 1
        ;;
esac