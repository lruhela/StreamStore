# StreamStore

A minimal event-driven order pipeline built with **Apache Kafka** (KRaft mode, no ZooKeeper) and Python. A producer publishes order events to a Kafka topic; a consumer tracks them with manual offset commits for at-least-once processing.

## Architecture

```
 producer.py                    Kafka (KRaft, single broker)                 tracker.py
┌───────────────┐    produce    ┌──────────────────────────┐    consume    ┌───────────────────┐
│  Order event  │ ─────────────▶│   topic: orders          │──────────────▶│  Consumer group:   │
│  (JSON)       │                │   1 partition, RF=1       │                │  order-tracker      │
└───────────────┘                └──────────────────────────┘                └───────────────────┘
                                                                                        │
                                                                              manual commit after
                                                                              each processed message
```

- **Producer** (`producer.py`) builds an order (`order_id`, `user`, `item`, `quantity`), serializes it as JSON, and publishes it to the `orders` topic.
- **Broker** (`docker-compose.yaml`) runs a single-node Kafka cluster in KRaft mode — no ZooKeeper required.
- **Consumer** (`tracker.py`) subscribes to `orders` as part of the `order-tracker` consumer group, reads from the earliest offset, and commits each offset manually only after the message is processed (`enable.auto.commit: False`) to avoid losing events on a crash.

## Tech Stack

- Python 3.9+
- [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) (librdkafka bindings)
- Apache Kafka 7.8.3 (Confluent image, KRaft mode)
- Docker Compose

## Prerequisites

- Docker + Docker Compose
- Python 3.9+

## Getting Started

**1. Start Kafka**

```bash
docker compose up -d
```

**2. Create a virtual environment and install dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**3. Run the consumer** (in one terminal — it will wait for messages)

```bash
python tracker.py
```

**4. Publish an order** (in another terminal)

```bash
python producer.py
```

The tracker should print something like:

```
Received order: 10 x quinoa salad bowl for user nicosddle (Order ID: cb1933bf-16ff-424c-bff6-4293eb842da7)
```

## Configuration

`tracker.py` reads its Kafka connection settings from environment variables:

| Variable                  | Default          | Description                          |
|----------------------------|-------------------|--------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `127.0.0.1:9092`  | Kafka broker address                 |
| `KAFKA_GROUP_ID`           | `order-tracker`   | Consumer group ID                    |

## Project Structure

```
.
├── docker-compose.yaml   # Single-node Kafka broker (KRaft mode)
├── producer.py           # Publishes a sample order to the `orders` topic
├── tracker.py            # Consumes and prints orders, with manual offset commits
└── requirements.txt      # Python dependencies
```

## Design Notes

- **Manual offset commits**: the consumer only commits an offset after successfully processing the message, giving at-least-once delivery semantics instead of Kafka's default at-most-once-on-crash behavior with auto-commit.
- **KRaft mode**: the broker runs without ZooKeeper, using Kafka's built-in Raft-based metadata quorum.

## Possible Improvements

- Schema validation for order payloads (e.g. with Avro/Protobuf + Schema Registry)
- Dead-letter topic for messages that fail processing
- Multiple partitions + multiple consumer instances to demonstrate rebalancing
- Persist tracked orders to a database instead of stdout
