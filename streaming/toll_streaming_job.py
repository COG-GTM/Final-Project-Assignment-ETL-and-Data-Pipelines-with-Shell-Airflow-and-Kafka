"""Flink streaming job that replaces the legacy ``streaming_data_reader.py``.

Pipeline:

    Kafka ``toll`` topic
        -> parse (streaming.source / streaming.deserialize)
        -> [raw records]  -> livetolldata sink (file or JDBC)
        -> assign event-time watermarks (streaming.transform)
        -> tumbling window count per plaza, with allowed lateness
        -> [windowed counts] -> plaza_counts sink (file or JDBC)

Exactly-once is achieved by enabling Flink checkpointing in ``EXACTLY_ONCE``
mode: the ``KafkaSource`` commits offsets on checkpoint and the ``FileSink`` /
JDBC sinks commit transactionally, so a failure replays from the last checkpoint
without duplicating or dropping records.

Run::

    python -m streaming.toll_streaming_job --bootstrap-servers localhost:9092 \
        --topic toll --sink file --output-dir output

See ``STREAMING.md`` for the full end-to-end local run instructions.
"""

from __future__ import annotations

import argparse
import logging

from pyflink.common import RestartStrategies
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment

from streaming import jars
from streaming.config import StreamingConfig
from streaming.sink import (
    attach_count_file_sink,
    attach_count_jdbc_sink,
    attach_record_file_sink,
    attach_record_jdbc_sink,
)
from streaming.source import kafka_parsed_stream
from streaming.transform import assign_watermarks, vehicle_count_per_plaza

LOG = logging.getLogger(__name__)


def configure_environment(config: StreamingConfig) -> StreamExecutionEnvironment:
    """Create and configure the streaming execution environment.

    Enables exactly-once checkpointing and a fixed-delay restart strategy so the
    job recovers from transient failures.
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    # --- Exactly-once checkpointing ---
    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(500)
    checkpoint_config.set_checkpoint_timeout(60000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    if config.checkpoint_dir:
        checkpoint_config.set_checkpoint_storage_dir(config.checkpoint_dir)

    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 5000))
    return env


def add_connector_jars(env: StreamExecutionEnvironment, config: StreamingConfig) -> None:
    """Add the Kafka (+ JDBC) connector JARs to the job classpath."""
    urls = list(jars.kafka_jar_urls())
    if config.sink_type == "jdbc":
        urls += jars.jdbc_jar_urls()
    env.add_jars(*urls)


def build_pipeline(env: StreamExecutionEnvironment, config: StreamingConfig) -> None:
    """Wire source -> transform -> sinks onto ``env`` (no execution)."""
    parsed = kafka_parsed_stream(env, config)

    # Raw record sink -> livetolldata (mirrors the legacy consumer's insert).
    if config.sink_type == "jdbc":
        attach_record_jdbc_sink(parsed, config)
    else:
        attach_record_file_sink(parsed, config)

    # Windowed aggregation -> per-plaza vehicle counts.
    watermarked = assign_watermarks(parsed, config)
    counts = vehicle_count_per_plaza(watermarked, config)
    if config.sink_type == "jdbc":
        attach_count_jdbc_sink(counts, config)
    else:
        attach_count_file_sink(counts, config)


def run(config: StreamingConfig) -> None:
    """Build and execute the streaming job (blocks until the job finishes)."""
    env = configure_environment(config)
    add_connector_jars(env, config)
    build_pipeline(env, config)
    LOG.info(
        "Starting toll streaming job: topic=%s bootstrap=%s sink=%s",
        config.topic,
        config.bootstrap_servers,
        config.sink_type,
    )
    env.execute("toll-streaming-job")


def _parse_args(argv=None) -> StreamingConfig:
    parser = argparse.ArgumentParser(description="Toll Kafka -> Flink streaming job")
    parser.add_argument("--bootstrap-servers", dest="bootstrap_servers")
    parser.add_argument("--topic")
    parser.add_argument("--group-id", dest="group_id")
    parser.add_argument("--starting-offsets", dest="starting_offsets", choices=["earliest", "latest"])
    parser.add_argument("--sink", dest="sink_type", choices=["file", "jdbc"])
    parser.add_argument("--output-dir", dest="output_dir")
    parser.add_argument("--bounded", action="store_true", help="Stop after consuming current offsets")
    parser.add_argument("--window-seconds", dest="window_size_seconds", type=int)
    parser.add_argument("--allowed-lateness-seconds", dest="allowed_lateness_seconds", type=int)
    parser.add_argument("--checkpoint-interval-ms", dest="checkpoint_interval_ms", type=int)
    args = parser.parse_args(argv)

    config = StreamingConfig()
    for name, value in vars(args).items():
        if value is not None and value is not False:
            setattr(config, name, value)
    return config


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(_parse_args(argv))


if __name__ == "__main__":
    main()
