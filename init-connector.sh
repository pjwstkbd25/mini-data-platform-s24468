#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."
while [ $(curl -s -o /dev/null -w %{http_code} http://connect:8083/connectors) -ne 200 ]
do
  sleep 1
done

echo "Creating Debezium connector..."
curl -X POST http://connect:8083/connectors \
  -H "Content-Type: application/json" \
  --data '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "db",
      "database.port": "5432",
      "database.user": "Jarek",
      "database.password": "Jarek",
      "database.dbname": "data",
      "database.server.name": "data_server",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.autocreate.mode": "filtered",
      "schema.include.list": "public",
      "table.include.list": "public.educational_data",
      "topic.prefix": "data_source"
    }
  }'

echo "Connector created successfully!"
