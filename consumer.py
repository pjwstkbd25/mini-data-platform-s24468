"""
Przykładowy konsument AVRO – Task 3 Deliverable
pip install confluent-kafka[avro]
"""
from confluent_kafka.avro import AvroConsumer

conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "demo-reader",
    "auto.offset.reset": "earliest",
    "schema.registry.url": "http://localhost:8081"
}

c = AvroConsumer(conf)
c.subscribe(["data_source.public.educational_data"])

print("Listening...")
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Err:", msg.error())
        continue
    print("Key:", msg.key(), "\nValue:", msg.value(), "\n---")
