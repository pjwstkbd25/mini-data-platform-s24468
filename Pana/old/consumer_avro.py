"""
Avro consumer -> topic 'users.avro'
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "users.avro"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def dict_from_user(obj, ctx):
    # nic nie mapujemy, zwracamy dict
    return obj


def main() -> None:
    # 1) Schema Registry client
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 2) deserializer (używamy writer schema z SR)
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=None,
        from_dict=dict_from_user,
    )

    # 3) consumer
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "users.avro.group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    print(f"Consuming from '{TOPIC}' (CTRL+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            user = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            print(f"key={msg.key()} value={user}")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()