"""An AI agent as a Kafka consumer.

Reads each fraud alert, asks Claude for a short review note, and writes the
note to a third topic. Nothing about Kafka changes: the agent is one more
consumer group that reads a topic and produces to another.

Needs ANTHROPIC_API_KEY in .env.

Run:  python agent_consumer.py
Stop: Ctrl+C
"""

import json
import os

import anthropic
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")
REVIEWS_TOPIC = os.getenv("KAFKA_REVIEWS_TOPIC", "fraud-reviews")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-5-5")

PROMPT = """You are a fraud analyst at a bank. A rule-based detector flagged this event:

{alert}

In two sentences, say how suspicious this looks and what the next step should be."""


def main():
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from the environment
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "fraud-agent",
            # Only review alerts that arrive after the agent starts,
            # so a restart does not spend API calls on old alerts.
            "auto.offset.reset": "latest",
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([ALERTS_TOPIC])
    print(f"Agent reading '{ALERTS_TOPIC}', writing to '{REVIEWS_TOPIC}'. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            alert = json.loads(msg.value())
            reply = client.messages.create(
                model=MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": PROMPT.format(alert=json.dumps(alert, indent=2))}],
            )
            note = reply.content[0].text.strip()

            review = {"alert": alert, "review": note, "model": MODEL}
            producer.produce(REVIEWS_TOPIC, key=msg.key(), value=json.dumps(review))
            producer.poll(0)

            txn = alert["transaction"]
            print(f"\n{txn['account_id']}  {','.join(alert['reasons'])}  ${txn['amount']:.2f}\n  {note}")
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
