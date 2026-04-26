import pandas as pd
from ETL.extract import finalize_dataframe, COMMON_COLUMNS


def test_finalize_dataframe_standardizes_schema():
    df = pd.DataFrame({
        "open_time": ["2024-01-01 00:00:00"],
        "close_time": ["2024-01-01 00:01:00"],
        "open": ["100"],
        "high": ["110"],
        "low": ["90"],
        "close": ["105"],
        "volume": ["12.5"],
    })

    result = finalize_dataframe(df, "binance", "BTCUSDT", "1m")

    assert list(result.columns) == COMMON_COLUMNS
    assert result.loc[0, "source"] == "binance"
    assert result.loc[0, "symbol"] == "BTCUSDT"
    assert result.loc[0, "interval"] == "1m"
    assert pd.api.types.is_numeric_dtype(result["open"])
    assert pd.api.types.is_datetime64_any_dtype(result["open_time"])