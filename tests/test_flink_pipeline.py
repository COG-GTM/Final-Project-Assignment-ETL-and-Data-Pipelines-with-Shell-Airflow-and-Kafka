"""Flink mini-cluster integration tests (no Kafka broker required).

These exercise the *real* parse -> watermark -> window -> sink path through a
local Flink mini-cluster using an in-memory collection source, so they can run
deterministically in CI without Docker or a Kafka broker. The Kafka end-to-end
test lives in ``test_kafka_e2e.py`` (skipped unless a broker is available).
"""

import glob
import os

import pytest

from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment

from streaming.config import StreamingConfig
from streaming.sink import attach_count_file_sink, attach_record_file_sink
from streaming.source import parsed_stream_from_strings
from streaming.transform import assign_watermarks, vehicle_count_per_plaza

# Three events for plaza 1 and one for plaza 2 in the 12:34 minute, then two
# more for plaza 1 in the 12:35 minute.
SAMPLE_LINES = [
    "Tue Jan 28 12:34:01 2022,1,car,1",
    "Tue Jan 28 12:34:20 2022,2,truck,1",
    "Tue Jan 28 12:34:55 2022,3,car,1",
    "Tue Jan 28 12:34:30 2022,4,car,2",
    "Tue Jan 28 12:35:05 2022,5,bus,1",
    "Tue Jan 28 12:35:40 2022,6,car,1",
    # A malformed line that must be dropped without crashing the job.
    "garbage,not,a,record,extra",
]


def _env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(500, CheckpointingMode.EXACTLY_ONCE)
    return env


def test_parse_path_drops_malformed_records():
    env = _env()
    parsed = parsed_stream_from_strings(env, SAMPLE_LINES)
    rows = [r for r in parsed.execute_and_collect()]
    # 6 valid records; the malformed line is filtered out.
    assert len(rows) == 6
    assert {r["plaza_id"] for r in rows} == {1, 2}


def test_windowed_vehicle_count_per_plaza():
    config = StreamingConfig()
    config.window_size_seconds = 60
    env = _env()

    parsed = parsed_stream_from_strings(env, SAMPLE_LINES)
    watermarked = assign_watermarks(parsed, config)
    counts = vehicle_count_per_plaza(watermarked, config)

    results = {
        (r["window_start"], r["plaza_id"]): r["vehicle_count"]
        for r in counts.execute_and_collect()
    }

    assert results[("2022-01-28 12:34:00", 1)] == 3
    assert results[("2022-01-28 12:34:00", 2)] == 1
    assert results[("2022-01-28 12:35:00", 1)] == 2


def _read_csv_files(base_dir):
    rows = []
    for path in glob.glob(os.path.join(base_dir, "**", "*.csv"), recursive=True):
        with open(path) as fh:
            rows.extend(line.strip() for line in fh if line.strip())
    return rows


def test_record_and_count_file_sinks(tmp_path):
    config = StreamingConfig()
    config.window_size_seconds = 60
    config.output_dir = str(tmp_path)
    env = _env()

    parsed = parsed_stream_from_strings(env, SAMPLE_LINES)
    attach_record_file_sink(parsed, config)
    watermarked = assign_watermarks(parsed, config)
    counts = vehicle_count_per_plaza(watermarked, config)
    attach_count_file_sink(counts, config)

    env.execute("test-file-sinks")

    records = _read_csv_files(os.path.join(str(tmp_path), "livetolldata"))
    assert len(records) == 6
    assert "2022-01-28 12:34:01,1,car,1" in records

    counts_out = _read_csv_files(os.path.join(str(tmp_path), "plaza_counts"))
    # 3 (plaza, window) groups expected.
    assert len(counts_out) == 3
    assert any(line.endswith(",1,3") and "12:34:00" in line for line in counts_out)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
