"""Real-broker end-to-end test: Kafka producer -> Flink job -> file sink.

This is the faithful end-to-end test required by the ticket. It needs an actual
Kafka broker and the Kafka connector JAR, so it is **skipped automatically**
unless:

* the ``KAFKA_BOOTSTRAP`` env var points at a reachable broker, and
* the Kafka connector JAR is present in ``jars/`` (run
  ``scripts/download_connectors.sh``), and
* ``kafka-python`` is installed.

To run it locally::

    scripts/download_connectors.sh
    # start a broker (see STREAMING.md), then:
    KAFKA_BOOTSTRAP=localhost:9092 python -m pytest tests/test_kafka_e2e.py -v

In CI the lighter mini-cluster tests in ``test_flink_pipeline.py`` cover the same
parse/window/sink logic without requiring a broker.
"""

import glob
import os
import uuid

import pytest

from streaming import jars

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")

pytestmark = pytest.mark.kafka

SAMPLE_MESSAGES = [
    "Tue Jan 28 12:34:01 2022,1,car,1",
    "Tue Jan 28 12:34:20 2022,2,truck,1",
    "Tue Jan 28 12:34:55 2022,3,car,1",
    "Tue Jan 28 12:34:30 2022,4,car,2",
    "Tue Jan 28 12:35:05 2022,5,bus,1",
    "Tue Jan 28 12:35:40 2022,6,car,1",
]


def _require_broker():
    if not KAFKA_BOOTSTRAP:
        pytest.skip("KAFKA_BOOTSTRAP not set; skipping real-broker e2e test")
    if not jars.has_kafka_connector():
        pytest.skip("Kafka connector JAR missing; run scripts/download_connectors.sh")
    try:
        import kafka  # noqa: F401
    except ImportError:
        pytest.skip("kafka-python not installed")


def _produce(topic):
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    for msg in SAMPLE_MESSAGES:
        producer.send(topic, msg.encode("utf-8"))
    producer.flush()
    producer.close()


def _read_csv_files(base_dir):
    rows = []
    for path in glob.glob(os.path.join(base_dir, "**", "*.csv"), recursive=True):
        with open(path) as fh:
            rows.extend(line.strip() for line in fh if line.strip())
    return rows


def test_kafka_to_flink_to_file_sink(tmp_path):
    _require_broker()

    topic = f"toll-e2e-{uuid.uuid4().hex[:8]}"
    _produce(topic)

    from streaming.config import StreamingConfig
    from streaming.toll_streaming_job import (
        add_connector_jars,
        build_pipeline,
        configure_environment,
    )

    config = StreamingConfig()
    config.bootstrap_servers = KAFKA_BOOTSTRAP
    config.topic = topic
    config.bounded = True  # stop after consuming the produced messages
    config.starting_offsets = "earliest"
    config.sink_type = "file"
    config.output_dir = str(tmp_path)
    config.window_size_seconds = 60

    env = configure_environment(config)
    add_connector_jars(env, config)
    build_pipeline(env, config)
    env.execute(f"toll-e2e-{topic}")

    records = _read_csv_files(os.path.join(str(tmp_path), "livetolldata"))
    assert len(records) == len(SAMPLE_MESSAGES)

    counts = _read_csv_files(os.path.join(str(tmp_path), "plaza_counts"))
    parsed_counts = {
        (parts[0], int(parts[2])): int(parts[3])
        for parts in (line.split(",") for line in counts)
    }
    assert parsed_counts[("2022-01-28 12:34:00", 1)] == 3
    assert parsed_counts[("2022-01-28 12:34:00", 2)] == 1
    assert parsed_counts[("2022-01-28 12:35:00", 1)] == 2
