from typing import Optional
from fastapi import FastAPI
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
import json, os

app = FastAPI()
producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_HOST", "127.0.0.1")+':'+os.getenv("KAFKA_PORT", "9092")],
    api_version=(0, 10, 1)
)


@app.get("/")
def main(loop: Optional[int] = None):
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y %H:%M:%S")

    if loop:
        for i in range(loop):
            message = "[" + current_time + "] Message Received {}".format(str(i))
            producer.send('febriano', json.dumps(message).encode('utf-8'))
            print("Message Send : ", i)
            sleep(2)
    else:
        # producer.send('febriano', b'hello, kafka testing home')
        message = "[" + current_time + "] Message Received"
        producer.send('febriano', json.dumps(message).encode('utf-8'))

    return {"Task": "Done"}


@app.get("/order/{order_id}")
def order(order_id: int, q: Optional[str] = None):
    return {"Order Id": order_id, "query": q}