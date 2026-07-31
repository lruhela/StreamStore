import json
import uuid

from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed for record {msg.key()}: {err}')
    else:
        print(f'Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} with value: {msg.value().decode("utf-8")}')
        print(dir(msg))

producer = Producer(producer_config)

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'nicosddle',
    'item': 'quinoa salad bowl',
    'quantity' : 10
}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic="orders", 
    value=value,
    callback=delivery_report
)

producer.flush()

