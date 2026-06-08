"""Configuration management for the toll ETL Flink batch job.

Supports CLI arguments, environment variables, and sensible defaults.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


@dataclass
class ETLConfig:
    """Configuration for the toll ETL batch job."""

    vehicle_csv_path: str
    tollplaza_tsv_path: str
    payment_txt_path: str
    output_path: str
    parallelism: int

    @classmethod
    def from_args(cls, args: list[str] | None = None) -> "ETLConfig":
        """Parse configuration from CLI arguments with env-var fallbacks."""
        parser = argparse.ArgumentParser(
            description="PyFlink batch ETL for toll plaza data consolidation"
        )
        parser.add_argument(
            "--vehicle-csv",
            default=os.environ.get("TOLL_VEHICLE_CSV", "vehicle-data.csv"),
            help="Path to vehicle-data.csv (default: $TOLL_VEHICLE_CSV or vehicle-data.csv)",
        )
        parser.add_argument(
            "--tollplaza-tsv",
            default=os.environ.get("TOLL_TOLLPLAZA_TSV", "tollplaza-data.tsv"),
            help="Path to tollplaza-data.tsv (default: $TOLL_TOLLPLAZA_TSV or tollplaza-data.tsv)",
        )
        parser.add_argument(
            "--payment-txt",
            default=os.environ.get("TOLL_PAYMENT_TXT", "payment-data.txt"),
            help="Path to payment-data.txt (default: $TOLL_PAYMENT_TXT or payment-data.txt)",
        )
        parser.add_argument(
            "--output",
            default=os.environ.get("TOLL_OUTPUT_PATH", "transformed_data.csv"),
            help="Output file path (default: $TOLL_OUTPUT_PATH or transformed_data.csv)",
        )
        parser.add_argument(
            "--parallelism",
            type=int,
            default=int(os.environ.get("TOLL_PARALLELISM", "1")),
            help="Flink job parallelism (default: $TOLL_PARALLELISM or 1)",
        )
        parsed = parser.parse_args(args)
        return cls(
            vehicle_csv_path=parsed.vehicle_csv,
            tollplaza_tsv_path=parsed.tollplaza_tsv,
            payment_txt_path=parsed.payment_txt,
            output_path=parsed.output,
            parallelism=parsed.parallelism,
        )
