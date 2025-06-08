#!/bin/bash

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
while [ $(curl -s -o /dev/null -w %{http_code} http://debezium:8083/connectors) -ne 200 ]
do
  sleep 1
done

# Create the connector
echo "Creating Debezium connector..."
curl -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  --data '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "myuser",
      "database.password": "mypassword",
      "database.dbname": "mydatabase",
      "database.server.name": "debezium",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.autocreate.mode": "filtered",
      "schema.include.list": "public",
      "table.include.list": "public.customers,public.products,public.orders",
      "topic.prefix": "debezium"
    }
  }'

echo "Connector created successfully!" 