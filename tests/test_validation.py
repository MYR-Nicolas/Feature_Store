from datetime import datetime, timezone, timedelta
import pandas as pd
from ETL.extract import validate_ohlcv_dataframe
from ETL.config import settings


def make_valid_df():
    return pd.DataFrame({
        "open_time": pd.date_range("2024-01-01", periods=10, freq="min", tz="UTC"),
        "open": [100] * 10,
        "high": [110] * 10,
        "low": [90] * 10,
        "close": [105] * 10,
        "volume": [1.5] * 10,
    })


def test_validate_valid_dataframe(monkeypatch):
    monkeypatch.setattr(settings, "VALIDATION_MIN_RATIO", 0.8)

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    df = make_valid_df()

    assert validate_ohlcv_dataframe(df, start_dt, end_dt) is True


def test_validate_rejects_duplicates(monkeypatch):
    monkeypatch.setattr(settings, "VALIDATION_MIN_RATIO", 0.8)

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    df = make_valid_df()
    df.loc[1, "open_time"] = df.loc[0, "open_time"]

    assert validate_ohlcv_dataframe(df, start_dt, end_dt) is False


def test_validate_rejects_invalid_ohlc(monkeypatch):
    monkeypatch.setattr(settings, "VALIDATION_MIN_RATIO", 0.8)

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    df = make_valid_df()
    df.loc[0, "high"] = 80

    assert validate_ohlcv_dataframe(df, start_dt, end_dt) is False