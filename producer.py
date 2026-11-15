"""The checkout service of an online store on Black Friday.

Every order placed becomes one event on a Kafka topic. Checkout does not
call the other services (email, inventory, fraud). It hands the event to
Kafka and moves on to the next customer.

Most orders are ordinary. Now and then two suspicious patterns are injected
for the fraud detector to find:
  - one very large order
  - a burst of 5 quick orders from the same customer

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
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

CUSTOMERS = [f"CUST-{n}" for n in range(1001, 1051)]  # 50 customers
PRODUCTS = [
    ("4K TV", "electronics"), ("Headphones", "electronics"), ("Laptop", "electronics"),
    ("Air Fryer", "home"), ("Robot Vacuum", "home"),
    ("Sneakers", "fashion"), ("Winter Jacket", "fashion"),
    ("LEGO Set", "toys"), ("Board Game", "toys"),
]
CITIES = ["Durham", "Raleigh", "Chapel Hill", "Charlotte", "New York", "Atlanta"]


def make_order(customer_id=None, amount=None) -> dict:
    product, category = random.choice(PRODUCTS)
    return {
        "order_id": uuid.uuid4().hex[:12],
        "customer_id": customer_id or random.choice(CUSTOMERS),
        "amount": amount or round(random.uniform(5, 900), 2),
        "currency": "USD",
        "category": category,
        "product": product,
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def next_batch() -> list[dict]:
    """Return the next orders to send: usually one, sometimes a suspicious pattern."""
    roll = random.random()
    if roll < 0.03:  # very large order
        return [make_order(amount=round(random.uniform(3000, 9000), 2))]
    if roll < 0.06:  # burst from one customer
        customer = random.choice(CUSTOMERS)
        return [make_order(customer_id=customer) for _ in range(5)]
    return [make_order()]


def on_delivery(err, msg):
    """Kafka calls this once per message to confirm where it was stored."""
    if err is not None:
        print(f"FAILED: {err}")
        return
    order = json.loads(msg.value())
    print(
        f"sent  partition={msg.partition()}  offset={msg.offset():<5}  "
        f"{order['customer_id']}  {order['amount']:>8.2f}  {order['category']:<11}  {order['product']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=1.0, help="orders per second")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP} at {args.rate}/s. Ctrl+C to stop.")
    try:
        while True:
            for i, order in enumerate(next_batch()):
                if i:
                    time.sleep(0.1)  # burst: 5 orders within half a second
                producer.produce(
                    TOPIC,
                    # The key decides the partition: same customer, same partition,
                    # so one customer's orders always stay in order.
                    key=order["customer_id"],
                    value=json.dumps(order),
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
