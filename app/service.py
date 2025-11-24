from kafka import KafkaProducer, KafkaConsumer
import json
import time

TOPIC = "demo-topic"

def produce_message():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(TOPIC, {"msg": "hello world"})
    producer.flush()

def consume_message():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="test-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    for msg in consumer:
        return msg.value
