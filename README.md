# Kafka Session: Hands-On Demo

A small real-time fraud pipeline for the data engineering session. A producer streams fake bank transactions into Kafka, a detector flags suspicious ones into a second topic, a dashboard summarizes both, and an optional AI agent reviews each alert.

```
producer.py ──> [transactions] ──> consumer.py        (prints every message)
                      │
                      ├──────────> fraud_detector.py ──> [fraud-alerts] ──> agent_consumer.py ──> [fraud-reviews]
                      │                                        │
                      └──────────> dashboard.py <──────────────┘
```

Names in `[brackets]` are Kafka topics. Everything else is a Python script.

**Step-by-step walkthrough:** https://androidilicious.github.io/kafka-session/

## Is it containerized?

- **Kafka, topic setup, and Kafka UI** always run in Docker containers.
- **The Python scripts** run on your laptop by default, so you can read, edit, and restart them easily.
- **Optional:** the producer and fraud detector can also run as containers (see "Run the apps in containers").
- **No Docker on your laptop?** Open the repo in GitHub Codespaces. The included dev container has Docker and Python ready.

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

Open Kafka UI at http://localhost:8080. You should see one cluster named `local` with three topics: `transactions`, `fraud-alerts`, `fraud-reviews`. They are created by the `kafka-init` container.

Look at one topic from the command line:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --describe --topic transactions --bootstrap-server localhost:9092
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

**Check:** Pick one account id in the producer output. Which partition does it go to? Does that ever change? Why?

### 2. Add a second consumer group

Leave A and B running. Terminal C:

```bash
python consumer.py --group audit
```

Terminal C receives every message too, including the ones sent before it started. The producer was not changed or restarted.

**Check:** In Kafka UI, open Consumers. How many groups are there, and what is the lag of each?

### 3. Kill a consumer and watch lag

Stop Terminal B with Ctrl+C. Leave the producer running for about 30 seconds.

In Kafka UI, open Consumers and click `printer`. The lag number climbs.

Start it again:

```bash
python consumer.py
```

It catches up from the offset where it stopped, then lag returns to 0.

**Check:** What was the lag just before you restarted? Were any messages lost?

### 4. Replay from the beginning

Use a group name Kafka has never seen:

```bash
python consumer.py --group replay-1
```

Every message since the topic was created prints again. Reading a message does not delete it.

**Check:** Why did `replay-1` start from offset 0 while `printer` resumed from where it left off?

### 5. Build a pipeline: detect fraud

Stop the extra consumers. Keep the producer running. Terminal B:

```bash
python fraud_detector.py
```

Terminal C:

```bash
python dashboard.py
```

The detector reads `transactions` and writes to `fraud-alerts`. The dashboard reads both topics with one consumer. In Kafka UI, open the `fraud-alerts` topic and look at the messages.

Now speed up the producer (stop it, then):

```bash
python producer.py --rate 5
```

**Check:** The detector keeps a per-account history in memory. If you ran two detectors in the same group, would one account's transactions ever be split between them? What guarantees that?

**Check:** Stop and restart `dashboard.py`. Its totals come back. Where did it get them from?

### 6. Add an AI agent (optional)

Put your key in `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...
```

Then:

```bash
python agent_consumer.py
```

For each new alert, the agent asks Claude for a two-sentence review and writes it to `fraud-reviews`. Open that topic in Kafka UI.

**Check:** What changed in Kafka to support the agent? What happens to incoming alerts if the agent is stopped for a minute and then restarted?

## Run the apps in containers

Instead of running `producer.py` and `fraud_detector.py` in terminals:

```bash
docker compose --profile apps up -d --build
docker compose logs -f producer fraud-detector
```

Inside the Docker network the broker address is `kafka:29092`, not `localhost:9092`. The compose file passes it in as `KAFKA_BOOTSTRAP_SERVERS`, so the scripts did not change. You can still run `consumer.py` or `dashboard.py` on your laptop against the same cluster.

## Using an AI assistant in this repo

Open the repo in Claude Code or Copilot Chat. `CLAUDE.md` tells the assistant how this lab is wired, and the scripts read all settings from `.env`. Do not paste the contents of `.env` into a chat.

Prompts to try:

- "Add a rule to the fraud detector: flag an account that transacts in two different cities within 60 seconds."
- "My consumer prints nothing. Help me find out why."
- "Write a consumer that saves every transaction to a DuckDB file."

## Clean up

```bash
docker compose --profile apps down -v
```

## Files

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Kafka broker (KRaft mode), topic setup, Kafka UI, optional app containers |
| `Dockerfile` | Image for the Python scripts |
| `.devcontainer/` | GitHub Codespaces setup |
| `.env.example` | Broker address, topic names, API key placeholder |
| `producer.py` | Streams fake transactions, with occasional suspicious patterns |
| `consumer.py` | Prints transactions; `--group` sets the consumer group |
| `fraud_detector.py` | Reads transactions, writes alerts to `fraud-alerts` |
| `dashboard.py` | Live totals in the terminal from two topics |
| `agent_consumer.py` | Claude reviews each alert, writes to `fraud-reviews` |
| `CLAUDE.md` | Context for AI coding assistants |
