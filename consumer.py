"""A downstream service that reacts to orders. Here it only prints them.

The group id is the service's name. Each group gets every order and keeps
its own place in the topic, so `notifications` and `inventory` never
compete with each other.

Run:                 python consumer.py                 (group: notifications)
Another service:     python consumer.py --group inventory
Replay everything:   python consumer.py --group any-new-name
Stop:                Ctrl+C
"""

import argparse
import json
import os

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
DEFAULT_GROUP = os.getenv("KAFKA_GROUP_ID", "notifications")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=DEFAULT_GROUP, help="consumer group id")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": args.group,
            # A group Kafka has never seen starts from the oldest message.
            # A group it knows resumes from its last committed offset.
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    print(f"Group '{args.group}' reading topic '{TOPIC}' on {BOOTSTRAP}. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"Topic '{TOPIC}' does not exist yet. Waiting...")
                else:
                    print(f"ERROR: {msg.error()}")
                continue

            order = json.loads(msg.value())
            print(
                f"got   partition={msg.partition()}  offset={msg.offset():<5}  "
                f"{order['customer_id']}  {order['amount']:>8.2f}  {order['category']:<11}  "
                f"{order['product']}, {order['city']}"
            )
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        consumer.close()  # commits offsets and leaves the group cleanly


if __name__ == "__main__":
    main()
