# Context for AI assistants

This repo is a teaching lab for an introductory Kafka session in a graduate data engineering course. Students are new to Kafka. The story is an online store on Black Friday: checkout publishes orders, and other services react to them.

## How the lab is wired

- `docker compose up -d` starts one Kafka broker in KRaft mode, a one-shot `kafka-init` container that creates the topics, and Kafka UI (http://localhost:8080).
- `docker compose --profile apps up -d --build` also runs `producer.py` and `fraud_detector.py` as containers.
- From the host, the broker is at `localhost:9092`. From inside the Docker network, it is at `kafka:29092`.
- Python scripts use `confluent-kafka` and read settings from `.env` through `python-dotenv`.
- Settings: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` (`orders`), `KAFKA_ALERTS_TOPIC` (`fraud-alerts`), `KAFKA_REVIEWS_TOPIC` (`fraud-reviews`), `KAFKA_GROUP_ID` (`notifications`), `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`.
- All three topics have 3 partitions. The message key is always `customer_id`.

## Data flow

- `producer.py` plays the checkout service and writes orders to `orders`. JSON fields: `order_id`, `customer_id`, `amount`, `currency`, `category`, `product`, `city`, `timestamp`.
- `fraud_detector.py` (group `fraud-detector`) reads `orders` and writes alerts to `fraud-alerts`. JSON fields: `reasons` (list of `LARGE_ORDER`, `VELOCITY`), `order`.
- `dashboard.py` (group `dashboard`) reads both topics and keeps totals in memory.
- `agent_consumer.py` (group `fraud-agent`) reads `fraud-alerts`, calls Claude, writes to `fraud-reviews`. JSON fields: `alert`, `review`, `model`.
- `consumer.py` stands in for a downstream service and prints `orders`; `--group` sets the group id (default `notifications`, the lab also uses `inventory`).
- The student walkthrough is `docs/index.html`, served by GitHub Pages. Keep it in step with the scripts.

## Rules

- Read configuration from environment variables. Never hardcode broker addresses, topic names, or API keys.
- Never read, print, or commit `.env`. Use `.env.example` to learn which variables exist.
- Keep code short and readable, with comments that explain the Kafka concept involved.
- When a student asks you to fix something, explain the cause first, then the fix.
- Stay with `confluent-kafka`. Do not add frameworks or new services unless asked.
