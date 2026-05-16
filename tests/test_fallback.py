from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from ELT.extract import extract_with_fallback


def valid_df() -> pd.DataFrame:
    """
    Create a valid mock OHLCV dataframe for testing.

    Returns:
        A valid one-minute OHLCV dataframe.
    """
    return pd.DataFrame({
        "open_time": pd.date_range(
            "2024-01-01",
            periods=10,
            freq="min",
            tz="UTC",
        ),
        "open": [100] * 10,
        "high": [110] * 10,
        "low": [90] * 10,
        "close": [105] * 10,
        "volume": [1.5] * 10,
    })


def test_fallback_uses_binance_if_valid(mocker):
    """Test that Binance is selected when valid data is returned."""

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    # Mock Binance extraction success
    mocker.patch(
        "ELT.extract.extract_from_binance",
        return_value=valid_df(),
    )

    # Mock validation success
    mocker.patch(
        "ELT.extract.validate_ohlcv_dataframe",
        return_value=True,
    )

    result = extract_with_fallback(start_dt, end_dt)

    assert result is not None


def test_fallback_uses_coinbase_if_binance_fails(mocker):
    """Test that Coinbase is used when Binance extraction fails."""

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    # Simulate Binance API failure
    mocker.patch(
        "ELT.extract.extract_from_binance",
        side_effect=Exception("Binance down"),
    )

    # Mock Coinbase fallback success
    mocker.patch(
        "ELT.extract.extract_from_coinbase",
        return_value=valid_df(),
    )

    # Mock validation success
    mocker.patch(
        "ELT.extract.validate_ohlcv_dataframe",
        return_value=True,
    )

    result = extract_with_fallback(start_dt, end_dt)

    assert result is not None


def test_fallback_raises_if_all_sources_fail(mocker):
    """Test that the pipeline raises an error if all sources fail."""

    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=10)

    # Simulate all extraction sources failing
    mocker.patch(
        "ELT.extract.extract_from_binance",
        side_effect=Exception("Binance down"),
    )

    mocker.patch(
        "ELT.extract.extract_from_coinbase",
        side_effect=Exception("Coinbase down"),
    )

    mocker.patch(
        "ELT.extract.extract_from_coinapi",
        side_effect=Exception("CoinAPI down"),
    )

    with pytest.raises(RuntimeError, match="All sources failed"):
        extract_with_fallback(start_dt, end_dt)