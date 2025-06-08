echo "Waiting for Kafka Connect to be ready..."
# Loop until the Connect REST API is responding (HTTP 200)
while [ "$(curl -s -o /dev/null -w '%{http_code}' http://connect:8083/connectors)" -ne 200 ]; do
  sleep 1
done

echo "Creating Debezium connector..."
curl -X POST http://connect:8083/connectors \
     -H "Content-Type: application/json" \
     --data @/init-connector.json

echo "Connector created successfully!"
