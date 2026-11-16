# Kafka Session: Hands-On Demo

It is Black Friday at an online store. Checkout publishes every order to Kafka, and the other services react on their own: one prints orders (standing in for notifications and inventory), a detector flags suspicious orders into a second topic, and a dashboard keeps live sales totals.

```
producer.py ──> [orders] ──> consumer.py        (notifications, inventory: prints every order)
(checkout)         │
                   ├───────> fraud_detector.py ──> [fraud-alerts]
                   │                                     │
                   └───────> dashboard.py <──────────────┘
```

Names in `[brackets]` are Kafka topics. Everything else is a Python script.

**Follow the step-by-step walkthrough:** https://androidilicious.github.io/kafka-session/

The walkthrough is the main guide for the session. It starts with the story, has an animated picture of the system, and gives commands for both macOS and Windows. The rest of this file is the short version.

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

python3 -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Activate the environment again in every new terminal.

Open Kafka UI at http://localhost:8080. You should see one cluster named `local` with three topics: `orders`, `fraud-alerts`, `fraud-reviews`. They are created by the `kafka-init` container. The walkthrough uses the first two.

## Exercises

Keep Kafka UI open in a browser tab the whole time. The step numbers match the walkthrough.

| Step | Run | What it shows |
| --- | --- | --- |
| 02 | Terminal A: `python producer.py` | A producer writes events; Kafka stores them with no reader running |
| 03 | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092` | A topic and its 3 partitions |
| 04 | Terminal B: `python consumer.py` | A consumer in group `notifications` reads every order |
| 05 | Terminal C: `python consumer.py --group inventory` | A second group gets every order too |
| 06 | Stop Terminal C for 30 seconds, check Consumers in Kafka UI, start it again | Lag grows, then the group catches up with nothing lost |
| 07 | Terminal D: `python consumer.py --group replay-1` | A new group replays the topic from offset 0 |
| 08 | Terminal A: `python producer.py --rate 5`, follow one customer id | The key always maps to the same partition |
| 09 | A second `python consumer.py --group inventory` | Consumers in one group split the partitions; stop one and they rebalance |
| 10 | `python fraud_detector.py` and `python dashboard.py` | Consume, process, produce; running totals from two topics |
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

- "Add a rule to the fraud detector: flag a customer who orders from two different cities within 60 seconds."
- "My consumer prints nothing. Help me find out why."
- "Write a consumer that saves every order to a DuckDB file."

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
| `producer.py` | The checkout service: streams fake orders, with occasional suspicious patterns |
| `consumer.py` | A downstream service that prints orders; `--group` sets the consumer group |
| `fraud_detector.py` | Reads orders, writes alerts to `fraud-alerts` |
| `dashboard.py` | Live sales totals in the terminal from two topics |
| `agent_consumer.py` | Optional extra, not part of the walkthrough: Claude reviews each alert, writes to `fraud-reviews` (needs `ANTHROPIC_API_KEY` in `.env`) |
| `docs/index.html` | The walkthrough page, served by GitHub Pages |
| `CLAUDE.md` | Context for AI coding assistants |
