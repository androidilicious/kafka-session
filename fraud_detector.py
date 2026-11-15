"""Read orders, apply two fraud rules, write alerts to a second topic.

This is the consume, process, produce pattern: one program is a consumer
of `orders` and a producer to `fraud-alerts` at the same time. One event
(an order) sets off the next one (an alert).

Rules:
  LARGE_ORDER  one order of 3000 or more
  VELOCITY     4 or more orders from one customer within 2 seconds

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
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")

LARGE_ORDER = 3000
VELOCITY_COUNT = 4
VELOCITY_WINDOW_SECONDS = 2

# Recent order times per customer. Because the message key is the customer
# id, all of one customer's orders arrive on one partition, so if you run
# two detectors in the same group, each customer is still seen by only one.
recent = defaultdict(deque)


def check(order: dict) -> list[str]:
    """Return the names of the rules this order breaks."""
    reasons = []
    if order["amount"] >= LARGE_ORDER:
        reasons.append("LARGE_ORDER")

    now = datetime.fromisoformat(order["timestamp"])
    times = recent[order["customer_id"]]
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

            order = json.loads(msg.value())
            reasons = check(order)
            if not reasons:
                continue

            alert = {"reasons": reasons, "order": order}
            producer.produce(ALERTS_TOPIC, key=order["customer_id"], value=json.dumps(alert))
            producer.poll(0)
            print(
                f"ALERT {','.join(reasons):<21} {order['customer_id']}  "
                f"{order['amount']:>8.2f}  {order['product']}, {order['city']}"
            )
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
