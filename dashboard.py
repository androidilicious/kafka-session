"""Live summary in the terminal, built from two topics at once.

One consumer can subscribe to several topics. This one reads `transactions`
and `fraud-alerts` and keeps running totals in memory.

Run:  python dashboard.py
Stop: Ctrl+C
"""

import json
import os
from collections import Counter

from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")
REFRESH_EVERY = 5  # messages

count = 0
total = 0.0
by_channel = Counter()
by_account = Counter()
alerts = Counter()


def render():
    print("\033[2J\033[H", end="")  # clear the terminal
    print(f"TRANSACTIONS  count={count}  total=${total:,.2f}\n")
    print("By channel")
    for channel, amount in by_channel.most_common():
        print(f"  {channel:<8} ${amount:>12,.2f}")
    print("\nTop 5 accounts by spend")
    for account, amount in by_account.most_common(5):
        print(f"  {account:<10} ${amount:>12,.2f}")
    print("\nFraud alerts")
    for reason, n in alerts.most_common():
        print(f"  {reason:<14} {n}")
    if not alerts:
        print("  none yet (is fraud_detector.py running?)")


def main():
    global count, total
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "dashboard",
            # Totals live in memory, so on restart we rebuild them
            # by reading both topics again from the start.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC, ALERTS_TOPIC])
    print("Waiting for messages...")

    seen = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            data = json.loads(msg.value())
            if msg.topic() == ALERTS_TOPIC:
                alerts.update(data["reasons"])
            else:
                count += 1
                total += data["amount"]
                by_channel[data["channel"]] += data["amount"]
                by_account[data["account_id"]] += data["amount"]

            seen += 1
            if seen % REFRESH_EVERY == 0:
                render()
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
