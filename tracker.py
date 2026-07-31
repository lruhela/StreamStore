import json
import os
import uuid

from confluent_kafka import Consumer

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': os.getenv('KAFKA_GROUP_ID', 'order-tracker'),
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'max.poll.interval.ms': 600000,
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print(
    f"Consumer is running with group '{consumer_config['group.id']}' and subscribed to the 'orders' topic. Waiting for messages..."
)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(
            f"Received order: {order['quantity']} x {order['item']} for user {order['user']} (Order ID: {order['order_id']})"
        )
        consumer.commit(message=msg)
except KeyboardInterrupt:
    print("\n Stopping consumer...")
finally:
    consumer.close()
