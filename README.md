# Kafka Session: Hands-On Demo

A minimal Kafka setup for the data engineering session. One broker, one web UI, one producer, one consumer. The data is a stream of fake bank transactions.

## What you need

- Docker Desktop (running)
- Python 3.10 or newer

## Setup

```bash
git clone https://github.com/androidilicious/kafka-session.git
cd kafka-session

cp .env.example .env
docker compose up -d

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Open Kafka UI at http://localhost:8080. You should see one cluster named `local`.

Create the topic with 3 partitions:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic transactions --partitions 3 \
  --bootstrap-server localhost:9092
```

## Exercises

Keep Kafka UI open in a browser tab the whole time.

### 1. Produce and consume

Terminal A:

```bash
python producer.py
```

Terminal B:

```bash
python consumer.py
```

Each transaction sent in A prints in B within a second.

**Check:** In the producer output, pick one account id. Which partition does it go to? Does that ever change?

### 2. Add a second consumer group

Leave A and B running. Terminal C:

```bash
python consumer.py --group dashboard
```

Terminal C receives every message too, including the ones sent before it started. The producer was not changed or restarted.

**Check:** In Kafka UI, open Consumers. How many groups are there, and what is the lag of each?

### 3. Kill a consumer and watch lag

Stop Terminal B with Ctrl+C. Leave the producer running for about 30 seconds.

In Kafka UI, open Consumers and click `fraud-check`. The lag number climbs.

Start it again:

```bash
python consumer.py
```

It catches up from the offset where it stopped, then lag returns to 0.

**Check:** What was the lag just before you restarted? Were any messages lost?

### 4. Replay from the beginning

Terminal D, with a group name Kafka has never seen:

```bash
python consumer.py --group replay-1
```

Every message since the topic was created prints again. Reading a message does not delete it.

**Check:** Why did `replay-1` start from offset 0 while `fraud-check` resumed from where it left off?

## Using an AI assistant in this repo

Open the repo in Claude Code or Copilot Chat. `CLAUDE.md` tells the assistant how this lab is wired, and the scripts read all settings from `.env`. Do not paste the contents of `.env` into a chat.

Prompts to try:

- "Write a consumer that keeps a running total per account and prints it every 10 messages."
- "My consumer prints nothing. Help me find out why."
- "Add a second topic called fraud-alerts and send transactions over 1500 to it."

## Clean up

```bash
docker compose down -v
```

## Files

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Kafka broker (KRaft mode) and Kafka UI |
| `.env.example` | Broker address, topic name, default group id |
| `producer.py` | Sends one fake transaction per second |
| `consumer.py` | Reads and prints transactions |
| `CLAUDE.md` | Context for AI coding assistants |
