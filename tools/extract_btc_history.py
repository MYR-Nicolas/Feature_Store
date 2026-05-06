import logging
from datetime import datetime, timezone
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

    end_dt = datetime(2026, 4, 26, 23, 59, tzinfo=timezone.utc)
    start_dt = datetime(2025, 4, 27, 0, 0, tzinfo=timezone.utc)

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