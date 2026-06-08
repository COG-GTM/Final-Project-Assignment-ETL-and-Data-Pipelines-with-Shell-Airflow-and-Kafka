"""PyFlink streaming replacement for the legacy ``streaming_data_reader.py``.

This package implements the streaming half of the IBM "ETL and Data Pipelines
with Shell, Airflow and Kafka" capstone (Tasks 2.1-2.9) as an Apache Flink
streaming application.

The legacy consumer (documented only via ``streaming_reader_code.png``) polled
the ``toll`` Kafka topic with a standalone ``kafka-python`` consumer and inserted
each message into a MySQL ``livetolldata`` table. This package replaces it with a
Flink streaming job that uses Flink's native ``flink-connector-kafka``, applies an
event-time windowed aggregation, and writes to a JDBC or filesystem sink with
exactly-once checkpointing.

The building blocks are intentionally small and importable so that a follow-up
ticket can unify this streaming job with the batch job under Flink's Table API:

* :mod:`streaming.deserialize` -- parse a raw ``toll`` message into a record.
* :mod:`streaming.source`      -- build the Kafka (or collection) source.
* :mod:`streaming.transform`   -- watermarks + windowed aggregation.
* :mod:`streaming.sink`        -- file / JDBC sinks.
* :mod:`streaming.config`      -- runtime configuration.
* :mod:`streaming.toll_streaming_job` -- wires it all together.
"""

from streaming.deserialize import TollRecord, parse_toll_message

__all__ = ["TollRecord", "parse_toll_message"]
