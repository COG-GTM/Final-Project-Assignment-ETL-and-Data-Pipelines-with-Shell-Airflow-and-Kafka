"""Parse raw ``toll`` topic messages into structured records.

The Toll Traffic Simulator (the producer, unchanged by this ticket) emits one
CSV-style line per vehicle passage:

    timestamp, vehicle_id, vehicle_type, toll_plaza_id

where ``timestamp`` is formatted with the C ``asctime`` style used by the IBM
lab, e.g. ``Tue Jan 28 12:34:56 2022`` (``%a %b %d %H:%M:%S %Y``).

The legacy ``streaming_data_reader.py`` (see ``streaming_reader_code.png``) did::

    (timestamp, vehicle_id, vehicle_type, plaza_id) = message.split(",")
    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("insert into livetolldata values(%s,%s,%s,%s)", ...)

This module reproduces that parsing/normalisation as a *pure* function so it can
be unit-tested without a running Flink cluster and reused by both the streaming
and (future) Table-API batch jobs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

LOG = logging.getLogger(__name__)

# Format the simulator emits, and the normalised DB format used by livetolldata.
SIMULATOR_TS_FORMAT = "%a %b %d %H:%M:%S %Y"
DB_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class TollRecord:
    """A single parsed toll event.

    Attributes:
        timestamp: Normalised ``%Y-%m-%d %H:%M:%S`` string (matches livetolldata).
        event_time_ms: Epoch milliseconds, used for event-time watermarking.
        vehicle_id: Numeric vehicle identifier.
        vehicle_type: Vehicle category (e.g. ``car``, ``truck``).
        plaza_id: Toll plaza identifier.
    """

    timestamp: str
    event_time_ms: int
    vehicle_id: int
    vehicle_type: str
    plaza_id: int

    def as_row_values(self):
        """Return values in livetolldata column order."""
        return (self.timestamp, self.vehicle_id, self.vehicle_type, self.plaza_id)


def _parse_timestamp(raw: str) -> Optional[datetime]:
    """Parse the simulator timestamp, tolerating the normalised DB format too."""
    raw = raw.strip()
    for fmt in (SIMULATOR_TS_FORMAT, DB_TS_FORMAT):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def parse_toll_message(message: str) -> Optional[TollRecord]:
    """Parse one raw ``toll`` message into a :class:`TollRecord`.

    Returns ``None`` (and logs a warning) for malformed messages so a single bad
    record cannot crash the streaming job -- the legacy consumer would have
    thrown and died.

    Args:
        message: Raw UTF-8 message value from the ``toll`` topic.
    """
    if message is None:
        return None

    parts = message.split(",")
    if len(parts) < 4:
        LOG.warning("Skipping malformed toll message (expected >=4 fields): %r", message)
        return None

    # Timestamp is the first field; the remaining 3 are id/type/plaza. We take
    # the last three fields and join everything before them as the timestamp so
    # an unexpected comma inside the timestamp does not shift the columns.
    raw_ts = ",".join(parts[:-3]).strip()
    raw_vehicle_id, raw_vehicle_type, raw_plaza_id = (p.strip() for p in parts[-3:])

    dateobj = _parse_timestamp(raw_ts)
    if dateobj is None:
        LOG.warning("Skipping toll message with unparseable timestamp %r", raw_ts)
        return None

    try:
        vehicle_id = int(raw_vehicle_id)
        plaza_id = int(raw_plaza_id)
    except ValueError:
        LOG.warning("Skipping toll message with non-numeric id/plaza: %r", message)
        return None

    normalised_ts = dateobj.strftime(DB_TS_FORMAT)
    # Treat the (naive) simulator timestamp as UTC for a stable epoch value.
    event_time_ms = int(dateobj.replace(tzinfo=timezone.utc).timestamp() * 1000)

    return TollRecord(
        timestamp=normalised_ts,
        event_time_ms=event_time_ms,
        vehicle_id=vehicle_id,
        vehicle_type=raw_vehicle_type,
        plaza_id=plaza_id,
    )
