import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, List, Tuple

import pandas as pd
import requests

from ETL.config import settings

# Global logger
logger = logging.getLogger(__name__)


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    """Return current UTC timestamp"""
    return datetime.now(timezone.utc)


def to_milliseconds(dt: datetime) -> int:
    """Convert datetime to milliseconds (Binance format)"""
    return int(dt.timestamp() * 1000)


def floor_to_minute(dt: datetime) -> datetime:
    """Round datetime down to the nearest minute"""
    return dt.replace(second=0, microsecond=0)


def isoformat_z(dt: datetime) -> str:
    """Format datetime to ISO string expected by Coinbase/CoinAPI"""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def get_default_time_window(days: Optional[int] = None) -> Tuple[datetime, datetime]:
    """
    Default extraction window (1 week by default)
    """
    days = days or settings.DEFAULT_DAYS
    end_dt = floor_to_minute(utc_now())
    start_dt = end_dt - timedelta(days=days)
    return start_dt, end_dt


# ==================
# HTTP RETRY
# ===============

def http_get_with_retry(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: Optional[int] = None,
    source_name: str = "unknown",
) -> requests.Response:
    """
    HTTP wrapper with retry mechanism.
    
    Handles:
    - network timeouts
    - temporary API failures
    - rate limiting issues
    """
    timeout = timeout or settings.TIMEOUT
    last_exception = None

    for attempt in range(1, settings.RETRY_COUNT + 1):
        try:
            logger.info("[%s] GET attempt=%s", source_name, attempt)

            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()

            return response

        except requests.RequestException as exc:
            last_exception = exc

            logger.warning("[%s] retry %s/%s error=%s",
                           source_name, attempt, settings.RETRY_COUNT, exc)

            time.sleep(settings.RETRY_SLEEP_SECONDS)

    raise RuntimeError(f"[{source_name}] HTTP failed: {last_exception}")


# =============================
# DATAFRAME STANDARDIZATION
# =============================

COMMON_COLUMNS = [
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "symbol",
    "interval",
    "source",
    "extracted_at",
]


def finalize_dataframe(df: pd.DataFrame, source: str, symbol: str, interval: str) -> pd.DataFrame:
    """
    Standardize all sources into a unified schema.
    
    Critical for:
    - Feature Store consistency
    - Model reproducibility
    - Pipeline modularity
    """

    if df is None or df.empty:
        return pd.DataFrame(columns=COMMON_COLUMNS)

    # Convert numeric columns
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], utc=True)

    # Add metadata (important for lineage and debugging)
    df["symbol"] = symbol
    df["interval"] = interval
    df["source"] = source
    df["extracted_at"] = pd.Timestamp.now(tz="UTC")

    # Final cleaning
    df = df[COMMON_COLUMNS]
    df = df.sort_values("open_time").drop_duplicates("open_time")

    return df.reset_index(drop=True)


# ===================
# DATA VALIDATION
# ===================

def expected_rows_for_minutes(start_dt: datetime, end_dt: datetime) -> int:
    """Compute expected number of candles"""
    return int((end_dt - start_dt).total_seconds() / 60)


def validate_ohlcv_dataframe(df, start_dt, end_dt) -> bool:
    """
    Validate data quality before accepting it.
    
    Prevents:
    - corrupted training data
    - missing time steps
    - structural inconsistencies
    """

    if df is None or df.empty:
        return False

    # Check required columns
    required = {"open_time", "open", "high", "low", "close", "volume"}
    if not required.issubset(df.columns):
        return False

    # Check duplicates
    if df["open_time"].duplicated().any():
        return False

    # Check OHLC consistency
    if (df["high"] < df["low"]).any():
        return False

    # Check data completeness
    expected = expected_rows_for_minutes(start_dt, end_dt)
    min_rows = int(expected * settings.VALIDATION_MIN_RATIO)

    return len(df) >= min_rows


# ===========================
# BINANCE (PRIMARY SOURCE)
# ===========================

def extract_from_binance(start_dt=None, end_dt=None):
    """
    Paginated extraction from Binance.
    
    Binance limit: 1000 candles per request.
    -> requires loop pagination
    """

    start_dt, end_dt = start_dt or get_default_time_window()[0], end_dt or get_default_time_window()[1]

    start_ms = to_milliseconds(start_dt)
    end_ms = to_milliseconds(end_dt)

    url = f"{settings.BINANCE_BASE_URL}/api/v3/klines"

    all_data = []
    current = start_ms

    while current < end_ms:

        params = {
            "symbol": settings.SYMBOL_BINANCE,
            "interval": settings.INTERVAL,
            "startTime": current,
            "limit": 1000
        }

        response = http_get_with_retry(url, params=params, source_name="binance")
        data = response.json()

        if not data:
            break

        all_data.extend(data)

        # Move forward in time
        last_time = data[-1][0]
        current = last_time + 60000  # +1 minute

        # Stop if last page reached
        if len(data) < 1000:
            break

    df = pd.DataFrame(all_data, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","tbav","tqav","ignore"
    ])

    return finalize_dataframe(df, "binance", settings.SYMBOL_BINANCE, settings.INTERVAL)


# =======================
# COINBASE (FALLBACK)
# =======================

def extract_from_coinbase(start_dt=None, end_dt=None):
    """
    Coinbase extraction.
    
    Limitation: max ~300 candles per request
    -> requires time window slicing
    """

    start_dt, end_dt = start_dt or get_default_time_window()[0], end_dt or get_default_time_window()[1]

    url = f"{settings.COINBASE_BASE_URL}/products/{settings.SYMBOL_COINBASE}/candles"

    current = start_dt
    all_data = []

    while current < end_dt:

        next_time = current + timedelta(minutes=300)

        params = {
            "start": isoformat_z(current),
            "end": isoformat_z(next_time),
            "granularity": 60
        }

        response = http_get_with_retry(url, params=params, source_name="coinbase")
        data = response.json()

        all_data.extend(data)

        current = next_time

    df = pd.DataFrame(all_data, columns=["time","low","high","open","close","volume"])

    df["open_time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df["close_time"] = df["open_time"] + pd.Timedelta(minutes=1)

    return finalize_dataframe(df, "coinbase", settings.SYMBOL_COINBASE, settings.INTERVAL)


# ==============================
# COINAPI ( FALLBACK)
# ==============================

def extract_from_coinapi(start_dt=None, end_dt=None):

    if not settings.COINAPI_KEY:
        raise RuntimeError("Missing COINAPI_KEY")

    start_dt, end_dt = start_dt or get_default_time_window()[0], end_dt or get_default_time_window()[1]

    url = f"{settings.COINAPI_BASE_URL}/v1/ohlcv/{settings.SYMBOL_COINAPI}/history"

    headers = {"X-CoinAPI-Key": settings.COINAPI_KEY}

    params = {
        "period_id": "1MIN",
        "time_start": isoformat_z(start_dt),
        "time_end": isoformat_z(end_dt)
    }

    response = http_get_with_retry(url, params=params, headers=headers, source_name="coinapi")
    data = response.json()

    df = pd.DataFrame(data)

    df = df.rename(columns={
        "time_period_start": "open_time",
        "time_period_end": "close_time",
        "price_open": "open",
        "price_high": "high",
        "price_low": "low",
        "price_close": "close",
        "volume_traded": "volume"
    })

    return finalize_dataframe(df, "coinapi", settings.SYMBOL_COINAPI, settings.INTERVAL)


# ==================
# FALLBACK LOGIC
# ==================

def extract_with_fallback(start_dt=None, end_dt=None):
    """
    Resilient extraction strategy:
    
    1. Binance (primary)
    2. Coinbase (fallback)
    3. CoinAPI (final fallback)
    """

    start_dt, end_dt = start_dt or get_default_time_window()[0], end_dt or get_default_time_window()[1]

    extractors = [
        ("binance", extract_from_binance),
        ("coinbase", extract_from_coinbase),
        ("coinapi", extract_from_coinapi),
    ]

    errors = []

    for name, func in extractors:
        try:
            logger.info(f"Trying source={name}")

            df = func(start_dt=start_dt, end_dt=end_dt)

            if validate_ohlcv_dataframe(df, start_dt, end_dt):
                logger.info(f"Selected source: {name}")
                return df

            errors.append(f"{name}: invalid data")

        except Exception as e:
            logger.warning(f"{name} failed: {e}")
            errors.append(f"{name}: {e}")

    raise RuntimeError("All sources failed: " + " | ".join(errors))


# ============================================================
# 9. LOCAL TEST ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    df = extract_with_fallback()

    print(df.head())
    print(df.shape)