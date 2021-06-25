import os

from kafka import KafkaConsumer
from json import loads
import json


if __name__ == '__main__':

    print("Kafka Consumer Application Started...")
    try:
        consumer = KafkaConsumer(
            'febriano',
            bootstrap_servers=[os.getenv("KAFKA_HOST", "127.0.0.1")+':'+os.getenv("KAFKA_PORT", "9092")],
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            api_version=(0, 10)
        )

        for meessage in consumer:
            print(meessage.value)
    except Exception as ex:
        print("Failed to read kafka message")
        print(ex)
