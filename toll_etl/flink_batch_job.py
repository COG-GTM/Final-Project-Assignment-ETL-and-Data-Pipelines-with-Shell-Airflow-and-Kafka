"""
PyFlink batch job replicating the ETL_toll_data.py Airflow DAG.

Pipeline:
  1. Read vehicle-data.csv → extract columns 1-4
  2. Read tollplaza-data.tsv → extract columns 5-7
  3. Read payment-data.txt (fixed-width) → extract payment fields (char 59+)
  4. Consolidate by Rowid (equivalent to positional `paste`)
  5. Transform: uppercase vehicle_type (column 4)
  6. Write sorted result to output CSV

Usage:
  python -m toll_etl.flink_batch_job \\
      --vehicle-csv vehicle-data.csv \\
      --tollplaza-tsv tollplaza-data.tsv \\
      --payment-txt payment-data.txt \\
      --output transformed_data.csv

Environment variables (fallbacks):
  TOLL_VEHICLE_CSV, TOLL_TOLLPLAZA_TSV, TOLL_PAYMENT_TXT,
  TOLL_OUTPUT_PATH, TOLL_PARALLELISM
"""

from __future__ import annotations

import os

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    MapFunction,
)
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.typeinfo import RowTypeInfo

from toll_etl.config import ETLConfig
from toll_etl.transforms import (
    extract_csv_fields,
    extract_tsv_fields,
    extract_fixed_width_fields,
    transform_vehicle_type,
    row_to_csv,
)

# Source identifiers for the tagged union
_SRC_CSV = 0
_SRC_TSV = 1
_SRC_FW = 2


class TagCSVSource(MapFunction):
    """Parse CSV line, tag with source=0 and extract rowid + payload."""

    def map(self, line: str) -> Row:
        fields = extract_csv_fields(line)
        payload = ",".join(fields)
        return Row(_SRC_CSV, fields[0], payload)


class TagTSVSource(MapFunction):
    """Parse TSV line, tag with source=1, extract rowid from col1 + payload."""

    def map(self, line: str) -> Row:
        line_stripped = line.rstrip("\r\n")
        rowid = line_stripped.split("\t")[0]
        tsv_fields = extract_tsv_fields(line)
        payload = ",".join(tsv_fields)
        return Row(_SRC_TSV, rowid, payload)


class TagFWSource(MapFunction):
    """Parse fixed-width line, tag with source=2, extract rowid + payload."""

    def map(self, line: str) -> Row:
        line_stripped = line.rstrip("\r\n")
        rowid = line_stripped.split()[0]
        fw_fields = extract_fixed_width_fields(line)
        payload = ",".join(fw_fields)
        return Row(_SRC_FW, rowid, payload)


class ConsolidateAndTransform(KeyedProcessFunction):
    """Keyed by rowid: collect all 3 source payloads, consolidate, transform, emit."""

    def open(self, ctx):
        self.parts_state = ctx.get_list_state(
            ListStateDescriptor("parts", Types.STRING())
        )

    def process_element(self, value, ctx):
        # Store "source_id:payload"
        self.parts_state.add(f"{value[0]}:{value[2]}")
        parts_list = list(self.parts_state.get())
        if len(parts_list) == 3:
            csv_payload = tsv_payload = fw_payload = ""
            for part in parts_list:
                src_str, payload = part.split(":", 1)
                src = int(src_str)
                if src == _SRC_CSV:
                    csv_payload = payload
                elif src == _SRC_TSV:
                    tsv_payload = payload
                else:
                    fw_payload = payload
            # Consolidate: csv(4 fields) + tsv(3 fields) + fw(2 fields) = 9 fields
            consolidated = f"{csv_payload},{tsv_payload},{fw_payload}"
            # Transform: uppercase field 4 (0-indexed 3 = vehicle_type)
            fields = tuple(consolidated.split(","))
            transformed = transform_vehicle_type(fields)
            yield row_to_csv(transformed)
            self.parts_state.clear()


def _read_text_lines(env, path: str, source_name: str):
    """Read a text file as a stream of lines.

    Uses the FileSource API (PyFlink >= 1.16). Each record is a single line
    with the line terminator stripped.
    """
    from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

    source = FileSource.for_record_stream_format(
        StreamFormat.text_line_format(), path
    ).build()
    return env.from_source(
        source, WatermarkStrategy.no_watermarks(), source_name
    )


def run_batch_job(config: ETLConfig) -> None:
    """Execute the Flink batch ETL pipeline.

    Reads three source files, extracts relevant columns, consolidates by Rowid,
    transforms vehicle_type to uppercase, and writes sorted output to a CSV file.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(config.parallelism)

    # Define tagged row type: (source_id: INT, rowid: STRING, payload: STRING)
    tagged_type = RowTypeInfo(
        [Types.INT(), Types.STRING(), Types.STRING()],
        ["source", "rowid", "payload"],
    )

    # Read and tag each source
    csv_tagged = _read_text_lines(
        env, config.vehicle_csv_path, "vehicle-csv-source"
    ).map(TagCSVSource(), output_type=tagged_type)
    tsv_tagged = _read_text_lines(
        env, config.tollplaza_tsv_path, "tollplaza-tsv-source"
    ).map(TagTSVSource(), output_type=tagged_type)
    fw_tagged = _read_text_lines(
        env, config.payment_txt_path, "payment-fw-source"
    ).map(TagFWSource(), output_type=tagged_type)

    # Union all sources, key by rowid, consolidate + transform
    unioned = csv_tagged.union(tsv_tagged, fw_tagged)
    keyed = unioned.key_by(lambda r: r[1])
    result_stream = keyed.process(
        ConsolidateAndTransform(), output_type=Types.STRING()
    )

    # Collect results, sort by rowid, and write to output file
    results = []
    with result_stream.execute_and_collect() as iterator:
        for line in iterator:
            results.append(line)

    # Sort by the integer rowid (first comma-separated field)
    results.sort(key=lambda line: int(line.split(",", 1)[0]))

    # Write to output file
    output_dir = os.path.dirname(config.output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(config.output_path, "w", newline="") as f:
        for line in results:
            f.write(line + "\n")

    print(f"Wrote {len(results)} rows to {config.output_path}")


def main() -> None:
    """Entry point for the Flink batch ETL job."""
    config = ETLConfig.from_args()
    run_batch_job(config)


if __name__ == "__main__":
    main()
