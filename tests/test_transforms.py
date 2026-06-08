"""Unit tests for toll_etl.transforms.

Tests each extraction step and the transformation step using small fixture files
that replicate the real data formats from tolldata.tgz.
"""

import pathlib

from toll_etl.transforms import (
    consolidate_row,
    extract_csv_fields,
    extract_fixed_width_fields,
    extract_tsv_fields,
    row_to_csv,
    transform_vehicle_type,
)

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


class TestExtractCSVFields:
    """Tests for extract_csv_fields (replicates cut -d',' -f1-4)."""

    def test_basic_row(self):
        line = "1,Thu Aug 19 21:54:38 2021,125094,car,2,VC965"
        result = extract_csv_fields(line)
        assert result == ("1", "Thu Aug 19 21:54:38 2021", "125094", "car")

    def test_strips_crlf(self):
        line = "2,Sat Jul 31 04:09:44 2021,174434,truck,2,VC965\r\n"
        result = extract_csv_fields(line)
        assert result == ("2", "Sat Jul 31 04:09:44 2021", "174434", "truck")

    def test_preserves_case(self):
        line = "4,Mon Sep 06 10:22:15 2021,992811,Car,3,VC432"
        result = extract_csv_fields(line)
        assert result[3] == "Car"

    def test_all_fixture_rows(self):
        filepath = FIXTURES_DIR / "vehicle-data.csv"
        with open(filepath) as f:
            lines = [ln for ln in f if ln.strip()]
        assert len(lines) == 5
        for line in lines:
            fields = extract_csv_fields(line)
            assert len(fields) == 4
            assert fields[0].isdigit()


class TestExtractTSVFields:
    """Tests for extract_tsv_fields (replicates cut -f5,6,7 | tr tab->comma | tr -d CR)."""

    def test_basic_row(self):
        line = "1\tThu Aug 19 21:54:38 2021\t125094\tcar\t2\t4856\tPC7C042B7\r\n"
        result = extract_tsv_fields(line)
        assert result == ("2", "4856", "PC7C042B7")

    def test_strips_cr(self):
        line = "2\tSat Jul 31 04:09:44 2021\t174434\ttruck\t2\t4154\tPC2C2EF9E\r"
        result = extract_tsv_fields(line)
        assert result == ("2", "4154", "PC2C2EF9E")

    def test_all_fixture_rows(self):
        filepath = FIXTURES_DIR / "tollplaza-data.tsv"
        with open(filepath) as f:
            lines = [ln for ln in f if ln.strip()]
        assert len(lines) == 5
        for line in lines:
            fields = extract_tsv_fields(line)
            assert len(fields) == 3


class TestExtractFixedWidthFields:
    """Tests for extract_fixed_width_fields (replicates cut -c59- | tr ' ' ',' | tr -d CR)."""

    def test_basic_row(self):
        line = "     1 Thu Aug 19 21:54:38 2021 125094     4856 PC7C042B7 PTE VC965"
        result = extract_fixed_width_fields(line)
        assert result == ("PTE", "VC965")

    def test_different_payment_code(self):
        line = "     2 Sat Jul 31 04:09:44 2021 174434     4154 PC2C2EF9E PTP VC965"
        result = extract_fixed_width_fields(line)
        assert result == ("PTP", "VC965")

    def test_all_fixture_rows(self):
        filepath = FIXTURES_DIR / "payment-data.txt"
        with open(filepath) as f:
            lines = [ln for ln in f if ln.strip()]
        assert len(lines) == 5
        for line in lines:
            fields = extract_fixed_width_fields(line)
            assert len(fields) == 2
            assert fields[0] in ("PTE", "PTP", "PTC")


class TestConsolidateRow:
    """Tests for consolidate_row (replicates paste -d',')."""

    def test_produces_9_fields(self):
        csv_fields = ("1", "Thu Aug 19 21:54:38 2021", "125094", "car")
        tsv_fields = ("2", "4856", "PC7C042B7")
        fw_fields = ("PTE", "VC965")
        result = consolidate_row(csv_fields, tsv_fields, fw_fields)
        assert len(result) == 9
        assert result == (
            "1",
            "Thu Aug 19 21:54:38 2021",
            "125094",
            "car",
            "2",
            "4856",
            "PC7C042B7",
            "PTE",
            "VC965",
        )

    def test_csv_format(self):
        row = ("1", "ts", "vn", "car", "2", "4856", "PC7C", "PTE", "VC965")
        assert row_to_csv(row) == "1,ts,vn,car,2,4856,PC7C,PTE,VC965"


class TestTransformVehicleType:
    """Tests for transform_vehicle_type (replicates awk toupper($4))."""

    def test_lowercase_to_upper(self):
        row = ("1", "ts", "vn", "car", "2", "4856", "PC7C", "PTE", "VC965")
        result = transform_vehicle_type(row)
        assert result[3] == "CAR"

    def test_mixed_case(self):
        row = ("4", "ts", "vn", "Car", "3", "5100", "PC12", "PTC", "VC432")
        result = transform_vehicle_type(row)
        assert result[3] == "CAR"

    def test_already_upper(self):
        row = ("5", "ts", "vn", "TRUCK", "4", "6200", "PCAB", "PTE", "VC101")
        result = transform_vehicle_type(row)
        assert result[3] == "TRUCK"

    def test_other_fields_unchanged(self):
        row = ("1", "ts", "vn", "car", "2", "4856", "PC7C", "PTE", "VC965")
        result = transform_vehicle_type(row)
        # Only field 3 should change
        assert result[:3] == row[:3]
        assert result[4:] == row[4:]


class TestEndToEndTransforms:
    """Integration test: full pipeline using fixture files (no Flink dependency)."""

    def test_full_pipeline_matches_shell_output(self):
        vehicle_path = FIXTURES_DIR / "vehicle-data.csv"
        tsv_path = FIXTURES_DIR / "tollplaza-data.tsv"
        payment_path = FIXTURES_DIR / "payment-data.txt"
        expected_path = FIXTURES_DIR / "expected_transformed_data.csv"

        with open(vehicle_path) as f:
            csv_lines = [ln for ln in f if ln.strip()]
        with open(tsv_path) as f:
            tsv_lines = [ln for ln in f if ln.strip()]
        with open(payment_path) as f:
            fw_lines = [ln for ln in f if ln.strip()]

        results = []
        for csv_line, tsv_line, fw_line in zip(csv_lines, tsv_lines, fw_lines):
            csv_fields = extract_csv_fields(csv_line)
            tsv_fields = extract_tsv_fields(tsv_line)
            fw_fields = extract_fixed_width_fields(fw_line)
            consolidated = consolidate_row(csv_fields, tsv_fields, fw_fields)
            transformed = transform_vehicle_type(consolidated)
            results.append(row_to_csv(transformed))

        with open(expected_path) as f:
            expected_lines = [ln.rstrip("\n") for ln in f if ln.strip()]

        assert results == expected_lines
