import logging
from datetime import timedelta
from pathlib import Path

from ELT.extract import (
    utc_now,
    floor_to_minute,
    extract_with_fallback,
    validate_ohlcv_dataframe,
)
from ELT.config import settings
from ELT.load import upload_to_gcs


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_btc_history_1y():
    end_dt = floor_to_minute(utc_now())
    start_dt = end_dt - timedelta(days=365)

    df = extract_with_fallback(start_dt=start_dt, end_dt=end_dt)

    if not validate_ohlcv_dataframe(df, start_dt, end_dt):
        raise ValueError("Historical BTC dataset validation failed")

    local_path = Path("data/btc_1m_history_1y.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        local_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    gcs_path = f"{settings.GCS_PREFIX}/history/btc_1m_history_1y.parquet"

    gcs_uri = upload_to_gcs(
        local_path=str(local_path),
        gcs_path=gcs_path,
    )

    logger.info("Backfill uploaded to %s", gcs_uri)

    return gcs_uri


if __name__ == "__main__":
    build_btc_history_1y()