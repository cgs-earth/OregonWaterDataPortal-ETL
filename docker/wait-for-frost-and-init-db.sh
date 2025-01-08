#!/bin/bash

# Wait for the frost-http service to be ready
echo "Waiting for frost-http to be ready..."

# Define a max number of retries and delay time
MAX_RETRIES=30
RETRY_INTERVAL=3
RETRY_COUNT=4

# Check for frost-http service readiness
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec -it frost-http curl -s --head http://localhost:8080/FROST-Server; then
        echo "frost-http is ready."
        break
    else
        echo "frost-http is not ready yet. Retrying in $RETRY_INTERVAL seconds..."
        RETRY_COUNT=$((RETRY_COUNT + 1))
        sleep $RETRY_INTERVAL
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "frost-http did not become ready in time. Exiting."
    exit 1
fi

# After frost-http is initialized, apply the SQL statements from the file
echo "Running SQL commands to create indices in the database..."

docker exec -i database psql -U sensorthings -d sensorthings < /indices.sql

echo "SQL commands executed successfully."
