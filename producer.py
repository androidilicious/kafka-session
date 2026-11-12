"""Send a stream of fake bank transactions to a Kafka topic.

Most transactions are ordinary. Now and then the producer injects two
suspicious patterns for the fraud detector to find:
  - one very large amount
  - a burst of 5 quick transactions from the same account

Run:    python producer.py
Faster: python producer.py --rate 5
Stop:   Ctrl+C
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

ACCOUNTS = [f"ACC-{n}" for n in range(1001, 1051)]  # 50 accounts
CHANNELS = ["card", "atm", "mobile", "branch"]
MERCHANTS = ["Harris Teeter", "Amazon", "Shell", "Uber", "Target", "Cosmic Cantina", "Delta", "Duke Stores"]
CITIES = ["Durham", "Raleigh", "Chapel Hill", "Charlotte", "New York", "Atlanta"]


def make_transaction(account_id=None, amount=None) -> dict:
    return {
        "txn_id": uuid.uuid4().hex[:12],
        "account_id": account_id or random.choice(ACCOUNTS),
        "amount": amount or round(random.uniform(5, 900), 2),
        "currency": "USD",
        "channel": random.choice(CHANNELS),
        "merchant": random.choice(MERCHANTS),
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def next_batch() -> list[dict]:
    """Return the next transactions to send: usually one, sometimes a suspicious pattern."""
    roll = random.random()
    if roll < 0.03:  # very large amount
        return [make_transaction(amount=round(random.uniform(3000, 9000), 2))]
    if roll < 0.06:  # burst from one account
        account = random.choice(ACCOUNTS)
        return [make_transaction(account_id=account) for _ in range(5)]
    return [make_transaction()]


def on_delivery(err, msg):
    """Kafka calls this once per message to confirm where it was stored."""
    if err is not None:
        print(f"FAILED: {err}")
        return
    txn = json.loads(msg.value())
    print(
        f"sent  partition={msg.partition()}  offset={msg.offset():<5}  "
        f"{txn['account_id']}  {txn['amount']:>8.2f}  {txn['channel']:<6}  {txn['merchant']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=1.0, help="transactions per second")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP} at {args.rate}/s. Ctrl+C to stop.")
    try:
        while True:
            for i, txn in enumerate(next_batch()):
                if i:
                    time.sleep(0.1)  # burst: 5 transactions within half a second
                producer.produce(
                    TOPIC,
                    # The key decides the partition: same account, same partition,
                    # so one account's transactions always stay in order.
                    key=txn["account_id"],
                    value=json.dumps(txn),
                    on_delivery=on_delivery,
                )
                producer.poll(0)  # triggers the delivery callback
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
