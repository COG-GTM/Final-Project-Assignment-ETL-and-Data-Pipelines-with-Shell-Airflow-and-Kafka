"""Shared Flink type definitions for the toll streaming pipeline.

Centralising the ``Row`` schemas keeps the source, transform and sink modules in
sync and makes the pipeline easy to lift into the Table API in a later ticket.
"""

from __future__ import annotations

from pyflink.common.typeinfo import Types

# Fields of a parsed toll record as it flows through the DataStream.
TOLL_RECORD_FIELD_NAMES = [
    "timestamp",
    "vehicle_id",
    "vehicle_type",
    "plaza_id",
    "event_time_ms",
]


def toll_record_type():
    """Row type for a parsed toll record (matches :class:`TollRecord`)."""
    return Types.ROW_NAMED(
        TOLL_RECORD_FIELD_NAMES,
        [Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.LONG()],
    )


# Fields emitted by the per-plaza windowed vehicle count.
PLAZA_COUNT_FIELD_NAMES = ["window_start", "window_end", "plaza_id", "vehicle_count"]


def plaza_count_type():
    """Row type for a windowed per-plaza vehicle count."""
    return Types.ROW_NAMED(
        PLAZA_COUNT_FIELD_NAMES,
        [Types.STRING(), Types.STRING(), Types.INT(), Types.LONG()],
    )
