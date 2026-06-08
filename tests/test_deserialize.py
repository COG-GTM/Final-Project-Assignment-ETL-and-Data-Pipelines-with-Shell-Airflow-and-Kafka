"""Unit tests for the pure-Python toll message parser."""

from datetime import datetime, timezone

from streaming.deserialize import (
    DB_TS_FORMAT,
    SIMULATOR_TS_FORMAT,
    TollRecord,
    parse_toll_message,
)


def test_parse_simulator_format_message():
    # Format the Toll Traffic Simulator emits: "%a %b %d %H:%M:%S %Y".
    msg = "Tue Jan 28 12:34:56 2022,1234567,car,4007"
    rec = parse_toll_message(msg)

    assert rec == TollRecord(
        timestamp="2022-01-28 12:34:56",
        event_time_ms=int(
            datetime(2022, 1, 28, 12, 34, 56, tzinfo=timezone.utc).timestamp() * 1000
        ),
        vehicle_id=1234567,
        vehicle_type="car",
        plaza_id=4007,
    )


def test_parse_normalises_timestamp_to_db_format():
    rec = parse_toll_message("Wed Feb 02 00:00:01 2022,1,truck,9")
    # Round-trips through the DB format.
    assert datetime.strptime(rec.timestamp, DB_TS_FORMAT)
    assert rec.as_row_values() == ("2022-02-02 00:00:01", 1, "truck", 9)


def test_parse_accepts_already_normalised_db_format():
    rec = parse_toll_message("2022-01-28 12:34:56,42,bus,7")
    assert rec.vehicle_id == 42
    assert rec.vehicle_type == "bus"
    assert rec.plaza_id == 7
    assert rec.timestamp == "2022-01-28 12:34:56"


def test_parse_strips_whitespace_around_fields():
    rec = parse_toll_message("Tue Jan 28 12:34:56 2022, 5 , van , 3 ")
    assert rec.vehicle_id == 5
    assert rec.vehicle_type == "van"
    assert rec.plaza_id == 3


def test_parse_returns_none_for_too_few_fields():
    assert parse_toll_message("Tue Jan 28 12:34:56 2022,1,car") is None


def test_parse_returns_none_for_bad_timestamp():
    assert parse_toll_message("not-a-date,1,car,2") is None


def test_parse_returns_none_for_non_numeric_ids():
    assert parse_toll_message("Tue Jan 28 12:34:56 2022,abc,car,2") is None
    assert parse_toll_message("Tue Jan 28 12:34:56 2022,1,car,xyz") is None


def test_parse_returns_none_for_none_input():
    assert parse_toll_message(None) is None


def test_timestamp_formats_are_consistent():
    # Guard against accidental edits to the format constants.
    assert SIMULATOR_TS_FORMAT == "%a %b %d %H:%M:%S %Y"
    assert DB_TS_FORMAT == "%Y-%m-%d %H:%M:%S"
