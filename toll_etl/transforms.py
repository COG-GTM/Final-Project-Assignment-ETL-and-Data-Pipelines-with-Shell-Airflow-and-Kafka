"""
Pure-Python extraction and transformation functions for the toll ETL pipeline.

These functions replicate the behavior of the shell commands in ETL_toll_data.py:
  - extract from CSV (cut -d"," -f1-4)
  - extract from TSV (cut -f5,6,7; tr tab->comma; strip CR)
  - extract from fixed-width (cut -c59-; tr space->comma; strip CR)
  - consolidate (paste -d",")
  - transform (toupper column 4)

All functions operate on individual lines/rows and are designed to be reusable
inside PyFlink UDFs or standalone.
"""

from __future__ import annotations

from typing import Tuple


def extract_csv_fields(line: str) -> Tuple[str, str, str, str]:
    """Extract columns 1-4 from a comma-separated line.

    Replicates: cut -d"," -f1-4 vehicle-data.csv
    Returns: (rowid, timestamp, vehicle_number, vehicle_type)
    """
    # Strip trailing CR/LF
    line = line.rstrip("\r\n")
    fields = line.split(",")
    # Fields 1-4 (0-indexed 0-3)
    return (fields[0], fields[1], fields[2], fields[3])


def extract_tsv_fields(line: str) -> Tuple[str, str, str]:
    """Extract columns 5-7 from a tab-separated line.

    Replicates: cut -f5,6,7 | tr '\\t' ',' | tr -d '\\r'
    Returns: (number_of_axles, tollplaza_id, tollplaza_code)
    """
    line = line.rstrip("\r\n")
    fields = line.split("\t")
    # Fields 5,6,7 (0-indexed 4,5,6)
    return (fields[4], fields[5], fields[6])


def extract_fixed_width_fields(line: str) -> Tuple[str, str]:
    """Extract payment fields from a fixed-width line starting at character 59.

    Replicates: cut -c59- | tr ' ' ',' | tr -d '\\r'
    Returns: (payment_type_code, vehicle_code)
    """
    line = line.rstrip("\r\n")
    # cut -c59- is 1-indexed, so Python slice [58:]
    tail = line[58:]
    # tr ' ' ',' then split on comma gives the fields
    parts = tail.replace(" ", ",").split(",")
    # Filter out empty strings that might appear from multiple spaces
    parts = [p for p in parts if p]
    return (parts[0], parts[1])


def consolidate_row(
    csv_fields: Tuple[str, str, str, str],
    tsv_fields: Tuple[str, str, str],
    fixed_width_fields: Tuple[str, str],
) -> Tuple[str, ...]:
    """Consolidate extracted fields into a single 9-column row.

    Replicates: paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv
    """
    return csv_fields + tsv_fields + fixed_width_fields


def transform_vehicle_type(row: Tuple[str, ...]) -> Tuple[str, ...]:
    """Uppercase the vehicle_type field (column 4, 0-indexed 3).

    Replicates: awk -F, -v OFS=, '{$4 = toupper($4); print}'
    """
    lst = list(row)
    lst[3] = lst[3].upper()
    return tuple(lst)


def row_to_csv(row: Tuple[str, ...]) -> str:
    """Convert a tuple row to a CSV line (no trailing newline)."""
    return ",".join(row)
