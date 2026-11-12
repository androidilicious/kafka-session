"""Read transactions, apply two fraud rules, write alerts to a second topic.

This is the consume, process, produce pattern: one program is a consumer
of `transactions` and a producer to `fraud-alerts` at the same time.

Rules:
  LARGE_AMOUNT  one transaction of 3000 or more
  VELOCITY      4 or more transactions from one account within 2 seconds

Run:  python fraud_detector.py
Stop: Ctrl+C
"""

import json
import os
from collections import defaultdict, deque
from datetime import datetime

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")

LARGE_AMOUNT = 3000
VELOCITY_COUNT = 4
VELOCITY_WINDOW_SECONDS = 2

# Recent transaction times per account. Because the message key is the
# account id, all of one account's events arrive on one partition, so if you
# run two detectors in the same group, each account is still seen by only one.
recent = defaultdict(deque)


def check(txn: dict) -> list[str]:
    """Return the names of the rules this transaction breaks."""
    reasons = []
    if txn["amount"] >= LARGE_AMOUNT:
        reasons.append("LARGE_AMOUNT")

    now = datetime.fromisoformat(txn["timestamp"])
    times = recent[txn["account_id"]]
    times.append(now)
    while (now - times[0]).total_seconds() > VELOCITY_WINDOW_SECONDS:
        times.popleft()
    if len(times) >= VELOCITY_COUNT:
        reasons.append("VELOCITY")
    return reasons


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "fraud-detector",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC])
    print(f"Reading '{TOPIC}', writing alerts to '{ALERTS_TOPIC}'. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"ERROR: {msg.error()}")
                continue

            txn = json.loads(msg.value())
            reasons = check(txn)
            if not reasons:
                continue

            alert = {"reasons": reasons, "transaction": txn}
            producer.produce(ALERTS_TOPIC, key=txn["account_id"], value=json.dumps(alert))
            producer.poll(0)
            print(
                f"ALERT {','.join(reasons):<22} {txn['account_id']}  "
                f"{txn['amount']:>8.2f}  {txn['merchant']}, {txn['city']}"
            )
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
