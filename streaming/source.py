"""Source builders for the toll streaming pipeline.

Two sources are provided:

* :func:`build_kafka_source` -- the production source using Flink's native
  ``flink-connector-kafka`` (``KafkaSource``), subscribing to the ``toll`` topic.
* :func:`parsed_stream_from_strings` / :func:`collection_source` -- helpers used
  by integration tests to feed raw lines through the *same* parsing path without
  needing a Kafka broker.

All of them ultimately yield a ``DataStream`` of parsed toll ``Row`` objects, so
the downstream transform and sink code is identical regardless of source.
"""

from __future__ import annotations

from typing import List

from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from streaming.config import StreamingConfig
from streaming.deserialize import parse_toll_message
from streaming.schema import toll_record_type


def _to_row(line: str):
    """Map a raw message string to a parsed toll ``Row`` (or ``None`` to drop)."""
    record = parse_toll_message(line)
    if record is None:
        return None
    return Row(
        timestamp=record.timestamp,
        vehicle_id=record.vehicle_id,
        vehicle_type=record.vehicle_type,
        plaza_id=record.plaza_id,
        event_time_ms=record.event_time_ms,
    )


def _parse_and_filter(raw_stream: DataStream) -> DataStream:
    """Parse a stream of raw strings into toll ``Row`` objects, dropping bad ones."""
    return (
        raw_stream.map(_to_row, output_type=toll_record_type())
        .filter(lambda row: row is not None)
        .name("parse_toll_messages")
    )


def build_kafka_source(config: StreamingConfig) -> KafkaSource:
    """Build a Flink ``KafkaSource`` subscribed to the ``toll`` topic.

    Uses ``set_value_only_deserializer`` so each record is the raw message string
    (matching the legacy ``msg.value.decode("utf-8")``). When ``config.bounded``
    is set the source stops at the latest offset -- handy for deterministic tests.
    """
    offset_initializer = (
        KafkaOffsetsInitializer.earliest()
        if config.starting_offsets == "earliest"
        else KafkaOffsetsInitializer.latest()
    )

    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(offset_initializer)
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if config.bounded:
        # Stop once the latest offsets (at job start) are reached.
        builder = builder.set_bounded(KafkaOffsetsInitializer.latest())
    return builder.build()


def kafka_parsed_stream(env: StreamExecutionEnvironment, config: StreamingConfig) -> DataStream:
    """Create a parsed toll ``Row`` stream from Kafka."""
    source = build_kafka_source(config)
    raw = env.from_source(
        source=source,
        watermark_strategy=_no_op_watermark(),
        source_name="toll-kafka-source",
    )
    return _parse_and_filter(raw)


def parsed_stream_from_strings(
    env: StreamExecutionEnvironment, lines: List[str]
) -> DataStream:
    """Create a parsed toll ``Row`` stream from an in-memory list of raw lines.

    Used by integration tests to exercise the real transform/sink path through a
    Flink mini-cluster without a Kafka broker.
    """
    raw = env.from_collection(lines, type_info=Types.STRING())
    return _parse_and_filter(raw)


def _no_op_watermark():
    """Watermark strategy for the source; real event-time watermarks are assigned
    later in :mod:`streaming.transform` after parsing the event timestamp."""
    from pyflink.common.watermark_strategy import WatermarkStrategy

    return WatermarkStrategy.no_watermarks()
