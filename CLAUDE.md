# Context for AI assistants

This repo is a teaching lab for an introductory Kafka session in a graduate data engineering course. Students are new to Kafka.

## How the lab is wired

- `docker compose up -d` starts one Kafka broker in KRaft mode and Kafka UI (http://localhost:8080).
- From the host, the broker is at `localhost:9092`. From inside the Docker network, it is at `kafka:29092`.
- Python scripts use `confluent-kafka` and read settings from `.env` through `python-dotenv`.
- Settings: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` (default `transactions`), `KAFKA_GROUP_ID` (default `fraud-check`).
- Messages are JSON with `account_id`, `amount`, `channel`, `timestamp`. The message key is `account_id`.
- The `transactions` topic has 3 partitions.

## Rules

- Read configuration from environment variables. Never hardcode broker addresses, topic names, or API keys.
- Never read, print, or commit `.env`. Use `.env.example` to learn which variables exist.
- Keep code short and readable, with comments that explain the Kafka concept involved.
- When a student asks you to fix something, explain the cause first, then the fix.
- Stay with `confluent-kafka`. Do not add frameworks or new services unless asked.
