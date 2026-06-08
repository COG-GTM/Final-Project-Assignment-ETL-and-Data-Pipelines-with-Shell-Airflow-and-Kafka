"""Runtime configuration for the toll streaming job.

Values are sourced from environment variables (so the job can be configured in
CI / containers without code changes) and can be overridden via CLI flags in
:mod:`streaming.toll_streaming_job`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# Kafka topic the Toll Traffic Simulator (producer) writes to. Do not change --
# the simulator is fixed by the assignment.
DEFAULT_TOPIC = "toll"

# Columns of the destination ``livetolldata`` table, mirroring the IBM lab schema:
#   timestamp DATETIME, vehicle_id INT, vehicle_type CHAR(15), plaza_id INT
LIVE_TOLL_COLUMNS = ("timestamp", "vehicle_id", "vehicle_type", "plaza_id")


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass
class StreamingConfig:
    """Configuration for the toll streaming pipeline."""

    # --- Kafka source ---
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", DEFAULT_TOPIC))
    group_id: str = field(default_factory=lambda: _env("KAFKA_GROUP_ID", "flink-toll-consumer"))
    # "earliest" or "latest"; tests use "earliest" with a bounded source.
    starting_offsets: str = field(default_factory=lambda: _env("KAFKA_STARTING_OFFSETS", "earliest"))
    # When True the Kafka source stops once it reaches the latest committed
    # offset. Used by integration tests so the job terminates deterministically.
    bounded: bool = False

    # --- Event-time / lateness handling ---
    # Bounded out-of-orderness for the watermark strategy (seconds).
    max_out_of_orderness_seconds: int = field(
        default_factory=lambda: int(_env("WATERMARK_OUT_OF_ORDERNESS_SECONDS", "5"))
    )
    # Allowed lateness applied to windows on top of the watermark (seconds).
    allowed_lateness_seconds: int = field(
        default_factory=lambda: int(_env("ALLOWED_LATENESS_SECONDS", "60"))
    )
    # Tumbling window size for the per-plaza vehicle count (seconds).
    window_size_seconds: int = field(
        default_factory=lambda: int(_env("WINDOW_SIZE_SECONDS", "60"))
    )

    # --- Checkpointing (exactly-once) ---
    checkpoint_interval_ms: int = field(
        default_factory=lambda: int(_env("CHECKPOINT_INTERVAL_MS", "10000"))
    )
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", ""))

    # --- Sink selection ---
    # "file" (filesystem, default / CI-friendly) or "jdbc".
    sink_type: str = field(default_factory=lambda: _env("SINK_TYPE", "file"))
    output_dir: str = field(default_factory=lambda: _env("OUTPUT_DIR", "output"))

    # --- JDBC sink (used only when sink_type == "jdbc") ---
    jdbc_url: str = field(
        default_factory=lambda: _env("JDBC_URL", "jdbc:mysql://localhost:3306/tolldata")
    )
    jdbc_driver: str = field(
        default_factory=lambda: _env("JDBC_DRIVER", "com.mysql.cj.jdbc.Driver")
    )
    jdbc_user: str = field(default_factory=lambda: _env("JDBC_USER", "root"))
    jdbc_password: str = field(default_factory=lambda: _env("JDBC_PASSWORD", ""))
    jdbc_table: str = field(default_factory=lambda: _env("JDBC_TABLE", "livetolldata"))
    jdbc_counts_table: str = field(
        default_factory=lambda: _env("JDBC_COUNTS_TABLE", "plaza_vehicle_counts")
    )

    @property
    def allowed_lateness_ms(self) -> int:
        return self.allowed_lateness_seconds * 1000

    @property
    def window_size_ms(self) -> int:
        return self.window_size_seconds * 1000
