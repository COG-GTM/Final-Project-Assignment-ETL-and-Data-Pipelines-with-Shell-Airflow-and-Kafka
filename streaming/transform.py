"""Event-time watermarking and windowed aggregation for the toll pipeline.

The aggregation demonstrates Flink streaming by counting vehicles per toll plaza
per tumbling event-time window (1 minute by default). Late-arriving events are
handled with a bounded-out-of-orderness watermark strategy plus an *allowed
lateness* on the window so events that arrive slightly after the watermark are
still counted instead of being silently dropped.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyflink.common import Duration, Row, Time
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from streaming.config import StreamingConfig
from streaming.schema import plaza_count_type


class _TollTimestampAssigner(TimestampAssigner):
    """Extract the event-time (epoch ms) from a parsed toll ``Row``."""

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        return int(value["event_time_ms"])


def assign_watermarks(stream: DataStream, config: StreamingConfig) -> DataStream:
    """Attach an event-time watermark strategy keyed on the ``timestamp`` field.

    Uses bounded-out-of-orderness so the pipeline tolerates events that are up to
    ``max_out_of_orderness_seconds`` late relative to the running maximum
    timestamp before the watermark advances past them.
    """
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(config.max_out_of_orderness_seconds)
    ).with_timestamp_assigner(_TollTimestampAssigner())
    return stream.assign_timestamps_and_watermarks(strategy).name("assign_watermarks")


class _CountVehiclesPerPlaza(ProcessWindowFunction):
    """Emit one row per (plaza, window) with the vehicle count."""

    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        window = context.window()
        count = sum(1 for _ in elements)
        yield Row(
            window_start=_fmt(window.start),
            window_end=_fmt(window.end),
            plaza_id=int(key),
            vehicle_count=int(count),
        )


def _fmt(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def vehicle_count_per_plaza(stream: DataStream, config: StreamingConfig) -> DataStream:
    """Windowed vehicle count per toll plaza.

    The input stream must already carry event-time watermarks (see
    :func:`assign_watermarks`). Applies a tumbling event-time window with the
    configured allowed lateness.
    """
    return (
        stream.key_by(lambda row: row["plaza_id"])
        .window(TumblingEventTimeWindows.of(Time.seconds(config.window_size_seconds)))
        .allowed_lateness(config.allowed_lateness_ms)
        .process(_CountVehiclesPerPlaza(), output_type=plaza_count_type())
        .name("vehicle_count_per_plaza")
    )
