from datetime import datetime, timezone
from ELT.extract import to_milliseconds, floor_to_minute, isoformat_z


def test_to_milliseconds():
    dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert to_milliseconds(dt) == 1704067200000


def test_floor_to_minute():
    dt = datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
    result = floor_to_minute(dt)

    assert result.second == 0
    assert result.microsecond == 0


def test_isoformat_z():
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert isoformat_z(dt) == "2024-01-01T12:00:00Z"