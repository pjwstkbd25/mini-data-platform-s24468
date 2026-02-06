"""
Avro producer -> topic 'users.avro'
"""

import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)

TOPIC = "users.avro"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def load_avro_schema_str(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def user_to_dict(user_obj, ctx):
    # nasz obiekt to już dict, więc po prostu go zwracamy
    return user_obj


def main() -> None:
    # 1) Schema Registry client
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 2) schema
    user_schema_str = load_avro_schema_str("../dags/user.avsc")

    # 3) serializer
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=user_schema_str,
        to_dict=user_to_dict,
    )

    key_serializer = StringSerializer("utf_8")

    # 4) producer
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    producer = Producer(producer_conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed for key={msg.key()}: {err}")
        else:
            print(
                f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
            )

    print(f"Producing Avro messages to topic '{TOPIC}'...")
    try:
        for i in range(5):
            user_id = str(uuid.uuid4())
            ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            user = {
                "id": user_id,
                "email": f"user{i}@example.com",
                "full_name": f"User {i}",
                "signup_ts": ts_ms,
            }

            value_bytes = avro_serializer(
                user, SerializationContext(TOPIC, MessageField.VALUE)
            )
            key_bytes = key_serializer(
                user_id, SerializationContext(TOPIC, MessageField.KEY)
            )

            producer.produce(
                topic=TOPIC,
                key=key_bytes,
                value=value_bytes,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(0.3)
    finally:
        print("Flushing...")
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()
