"""Sink builders for the toll streaming pipeline.

Two sink families are supported, mirroring the ticket's "JDBC sink, with a
filesystem sink as an acceptable fallback":

* Filesystem sinks (default, CI-friendly): write CSV lines via Flink's
  ``FileSink``. Commits are tied to checkpoints, preserving exactly-once.
* JDBC sinks: write the parsed records to the ``livetolldata`` table (mirroring
  the legacy consumer) and the windowed counts to ``plaza_vehicle_counts`` via
  Flink's ``flink-connector-jdbc``.

Each builder takes a ``DataStream`` and attaches a sink, so the main job can pick
a sink per output without duplicating wiring.
"""

from __future__ import annotations

import os

from pyflink.common import Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)

from streaming.config import StreamingConfig


# --------------------------------------------------------------------------- #
# CSV formatting helpers (also reused by tests).
# --------------------------------------------------------------------------- #
def format_record_csv(row: Row) -> str:
    """Format a parsed toll record as ``timestamp,vehicle_id,vehicle_type,plaza_id``."""
    return f"{row['timestamp']},{row['vehicle_id']},{row['vehicle_type']},{row['plaza_id']}"


def format_count_csv(row: Row) -> str:
    """Format a windowed count as ``window_start,window_end,plaza_id,vehicle_count``."""
    return (
        f"{row['window_start']},{row['window_end']},"
        f"{row['plaza_id']},{row['vehicle_count']}"
    )


# --------------------------------------------------------------------------- #
# Filesystem sinks.
# --------------------------------------------------------------------------- #
def _file_sink(base_path: str, prefix: str) -> FileSink:
    return (
        FileSink.for_row_format(base_path, Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix(prefix)
            .with_part_suffix(".csv")
            .build()
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )


def attach_record_file_sink(stream: DataStream, config: StreamingConfig) -> None:
    """Write parsed toll records as CSV files under ``<output_dir>/livetolldata``."""
    base_path = os.path.join(config.output_dir, "livetolldata")
    lines = stream.map(format_record_csv, output_type=Types.STRING()).name("format_records")
    lines.sink_to(_file_sink(base_path, "livetolldata")).name("record_file_sink")


def attach_count_file_sink(stream: DataStream, config: StreamingConfig) -> None:
    """Write windowed per-plaza counts as CSV files under ``<output_dir>/plaza_counts``."""
    base_path = os.path.join(config.output_dir, "plaza_counts")
    lines = stream.map(format_count_csv, output_type=Types.STRING()).name("format_counts")
    lines.sink_to(_file_sink(base_path, "plaza_counts")).name("count_file_sink")


# --------------------------------------------------------------------------- #
# JDBC sinks.
# --------------------------------------------------------------------------- #
def _jdbc_connection_options(config: StreamingConfig):
    from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions

    return (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(config.jdbc_url)
        .with_driver_name(config.jdbc_driver)
        .with_user_name(config.jdbc_user)
        .with_password(config.jdbc_password)
        .build()
    )


def _jdbc_execution_options():
    from pyflink.datastream.connectors.jdbc import JdbcExecutionOptions

    return (
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(100)
        .with_max_retries(3)
        .build()
    )


def attach_record_jdbc_sink(stream: DataStream, config: StreamingConfig) -> None:
    """Insert parsed records into the ``livetolldata`` table (legacy behaviour)."""
    from pyflink.datastream.connectors.jdbc import JdbcSink

    projected = stream.map(
        lambda row: Row(row["timestamp"], row["vehicle_id"], row["vehicle_type"], row["plaza_id"]),
        output_type=Types.ROW([Types.STRING(), Types.INT(), Types.STRING(), Types.INT()]),
    ).name("project_record")

    sql = (
        f"insert into {config.jdbc_table} "
        "(timestamp, vehicle_id, vehicle_type, plaza_id) values (?, ?, ?, ?)"
    )
    projected.add_sink(
        JdbcSink.sink(
            sql,
            Types.ROW([Types.STRING(), Types.INT(), Types.STRING(), Types.INT()]),
            _jdbc_connection_options(config),
            _jdbc_execution_options(),
        )
    ).name("record_jdbc_sink")


def attach_count_jdbc_sink(stream: DataStream, config: StreamingConfig) -> None:
    """Insert windowed counts into the ``plaza_vehicle_counts`` table."""
    from pyflink.datastream.connectors.jdbc import JdbcSink

    projected = stream.map(
        lambda row: Row(
            row["window_start"], row["window_end"], row["plaza_id"], row["vehicle_count"]
        ),
        output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.INT(), Types.LONG()]),
    ).name("project_count")

    sql = (
        f"insert into {config.jdbc_counts_table} "
        "(window_start, window_end, plaza_id, vehicle_count) values (?, ?, ?, ?)"
    )
    projected.add_sink(
        JdbcSink.sink(
            sql,
            Types.ROW([Types.STRING(), Types.STRING(), Types.INT(), Types.LONG()]),
            _jdbc_connection_options(config),
            _jdbc_execution_options(),
        )
    ).name("count_jdbc_sink")
