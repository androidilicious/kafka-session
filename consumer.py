"""Read transactions from a Kafka topic and print them.

Run:                 python consumer.py
Different group:     python consumer.py --group dashboard
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
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
DEFAULT_GROUP = os.getenv("KAFKA_GROUP_ID", "fraud-check")


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

            txn = json.loads(msg.value())
            flag = "  <-- LARGE" if txn["amount"] > 1500 else ""
            print(
                f"got   partition={msg.partition()}  offset={msg.offset()}  "
                f"{txn['account_id']}  {txn['amount']:>8.2f}  {txn['channel']}{flag}"
            )
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        consumer.close()  # commits offsets and leaves the group cleanly


if __name__ == "__main__":
    main()
