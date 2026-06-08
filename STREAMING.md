# Streaming pipeline: Flink replacement for `streaming_data_reader.py`

This document describes the streaming half of the capstone (README Tasks
2.1–2.9), reimplemented as an **Apache Flink (PyFlink) streaming job** that
replaces the legacy standalone Kafka consumer `streaming_data_reader.py`.

> In this repo `streaming_data_reader.py` never existed as real source — it was
> only documented via `streaming_reader_code.png` and the README task list. The
> Flink job in [`streaming/`](streaming/) is its replacement. Nothing in the repo
> depends on `streaming_data_reader.py` anymore.

## What it does

```
Kafka "toll" topic  (produced by the Toll Traffic Simulator — UNCHANGED)
        │
        ▼
  parse / deserialize           streaming/deserialize.py
        │   timestamp,vehicle_id,vehicle_type,toll_plaza_id
        │
        ├──────────────► livetolldata sink (file or JDBC)   streaming/sink.py
        │
        ▼
  assign event-time watermarks  streaming/transform.py
        │
        ▼
  tumbling 1-min window,
  vehicle count per plaza,
  with allowed lateness
        │
        ▼
  plaza_counts sink (file or JDBC)
```

* **Native Kafka connector** — uses Flink's `flink-connector-kafka`
  (`KafkaSource`) subscribed to the `toll` topic, value-only string
  deserialization (matching the legacy `msg.value.decode("utf-8")`).
* **Message schema** — `timestamp, vehicle_id, vehicle_type, toll_plaza_id`,
  where `timestamp` is the simulator's `%a %b %d %H:%M:%S %Y` format, normalised
  to `%Y-%m-%d %H:%M:%S` (the `livetolldata` DATETIME format).
* **Windowed aggregation** — vehicle count per toll plaza per minute
  (tumbling event-time window), demonstrating Flink streaming.
* **Exactly-once** — Flink checkpointing in `EXACTLY_ONCE` mode. `KafkaSource`
  commits offsets on checkpoint; `FileSink` / JDBC sinks commit transactionally,
  so a failure replays from the last checkpoint without dupes or loss.
* **Late data** — bounded-out-of-orderness watermark strategy on the event
  `timestamp` plus `allowed_lateness` on the window, so slightly-late events are
  still counted instead of dropped.
* **Sinks** — JDBC sink into MySQL `livetolldata` / `plaza_vehicle_counts`
  (mirrors the legacy insert), with a **filesystem sink as the default /
  CI-friendly fallback**.

The building blocks (`source`, `transform`, `sink`) are small, importable
functions so a follow-up ticket (Ticket 3) can unify this with the batch job
under Flink's Table API.

## Setup

Requires Python 3.8–3.11 and Java 8 or 11 (PyFlink 1.20 constraints).

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64   # a Java 8/11 home

# Download the Flink connector JARs (not committed; large binaries):
scripts/download_connectors.sh
```

## Run the tests

```bash
# Unit + Flink mini-cluster integration tests (no Kafka broker needed):
python -m pytest -v
```

The mini-cluster tests in `tests/test_flink_pipeline.py` exercise the real
parse → watermark → window → sink path through a local Flink cluster using an
in-memory source, so they run deterministically in CI without Docker.

## Run the full end-to-end test (real Kafka broker)

`tests/test_kafka_e2e.py` produces messages to a real broker, runs the Flink job
(bounded), and asserts the records + windowed counts land in the file sink. It is
skipped unless a broker is available.

Start a local single-node Kafka in **KRaft mode** (no Zookeeper needed):

```bash
# Using the Apache Kafka distribution (kafka_2.13-3.x):
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

Then:

```bash
scripts/download_connectors.sh
KAFKA_BOOTSTRAP=localhost:9092 python -m pytest tests/test_kafka_e2e.py -v
```

### Run the job for real

```bash
# Toll Traffic Simulator (the producer) keeps writing to the "toll" topic.
# Filesystem sink (default):
python -m streaming.toll_streaming_job \
    --bootstrap-servers localhost:9092 --topic toll \
    --sink file --output-dir output

# JDBC sink into MySQL livetolldata (mirrors the legacy consumer):
JDBC_URL="jdbc:mysql://localhost:3306/tolldata" JDBC_USER=root JDBC_PASSWORD=... \
python -m streaming.toll_streaming_job --topic toll --sink jdbc
```

The JDBC sink expects these tables:

```sql
CREATE TABLE livetolldata (
    timestamp    DATETIME,
    vehicle_id   INT,
    vehicle_type CHAR(15),
    plaza_id     INT
);

CREATE TABLE plaza_vehicle_counts (
    window_start  DATETIME,
    window_end    DATETIME,
    plaza_id      INT,
    vehicle_count BIGINT,
    -- Required for the idempotent upsert: with allowed_lateness a window
    -- re-fires when late data arrives, and the counts sink uses
    -- INSERT ... ON DUPLICATE KEY UPDATE so the latest count overwrites the
    -- previous one instead of appending a duplicate row.
    PRIMARY KEY (window_start, window_end, plaza_id)
);
```

## Note on Zookeeper / KRaft

Tasks 2.1–2.2 start Zookeeper and then the Kafka broker. The **Zookeeper
dependency can be removed** by migrating the broker to **KRaft mode** (Kafka's
built-in Raft metadata quorum, default in Kafka 3.3+). The Flink job is
unaffected — it only talks to the broker's bootstrap servers. This migration is
optional for this ticket; the local end-to-end instructions above already use
KRaft mode.
