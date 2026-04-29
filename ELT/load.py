import os
import pandas as pd
from sqlalchemy import create_engine, text


def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def prepare_raw_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert standardized extraction dataframe into raw.ohlcv schema.
    """

    raw = pd.DataFrame({
        "symbol": df["symbol"],
        "ts": df["open_time"],
        "timeframe": df["interval"],
        "open_price": df["open"],
        "high_price": df["high"],
        "low_price": df["low"],
        "close_price": df["close"],
        "volume": df["volume"],
        "source": df["source"],
        "ingested_at": df["extracted_at"],
        "is_closed": True,
    })

    return raw


def load_raw_ohlcv(df: pd.DataFrame) -> int:
    """
    Upsert OHLCV data into raw.ohlcv.
    """

    engine = get_engine()
    raw = prepare_raw_ohlcv(df)

    rows_loaded = 0

    sql = text("""
        INSERT INTO raw.ohlcv (
            symbol,
            ts,
            timeframe,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            source,
            ingested_at,
            is_closed
        )
        VALUES (
            :symbol,
            :ts,
            :timeframe,
            :open_price,
            :high_price,
            :low_price,
            :close_price,
            :volume,
            :source,
            :ingested_at,
            :is_closed
        )
        ON CONFLICT (symbol, ts, timeframe, source)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            ingested_at = EXCLUDED.ingested_at,
            is_closed = EXCLUDED.is_closed;
    """)

    records = raw.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(sql, records)
        rows_loaded = len(records)

    return rows_loaded