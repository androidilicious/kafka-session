"""Send one fake bank transaction per second to a Kafka topic.

Run: python producer.py
Stop: Ctrl+C
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

ACCOUNTS = ["ACC-1001", "ACC-1002", "ACC-1003", "ACC-1004", "ACC-1005"]
CHANNELS = ["card", "atm", "mobile", "branch"]


def make_transaction() -> dict:
    return {
        "account_id": random.choice(ACCOUNTS),
        "amount": round(random.uniform(5, 2000), 2),
        "channel": random.choice(CHANNELS),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def on_delivery(err, msg):
    """Kafka calls this once per message to confirm where it was stored."""
    if err is not None:
        print(f"FAILED: {err}")
        return
    print(
        f"sent  key={msg.key().decode()}  "
        f"partition={msg.partition()}  offset={msg.offset()}  "
        f"value={msg.value().decode()}"
    )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP}. Ctrl+C to stop.")
    try:
        while True:
            txn = make_transaction()
            producer.produce(
                TOPIC,
                # The key decides the partition: same account, same partition,
                # so one account's transactions always stay in order.
                key=txn["account_id"],
                value=json.dumps(txn),
                on_delivery=on_delivery,
            )
            producer.poll(0)  # triggers the delivery callback
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
