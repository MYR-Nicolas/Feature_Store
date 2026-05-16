import logging
from datetime import datetime, timezone
from pathlib import Path

from ELT.config import settings
from ELT.extract import (
    extract_with_fallback,
    floor_to_minute,
    utc_now,
    validate_ohlcv_dataframe,
)
from ELT.load import upload_to_gcs


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_btc_history_1y() -> str:
    """
    Build and upload a one-year BTC historical dataset.

    The workflow:
        - Extracts one year of BTC OHLCV data
        - Validates dataset quality
        - Saves the dataset as Parquet
        - Uploads the dataset to Google Cloud Storage

    Returns:
        The GCS URI of the uploaded historical dataset.

    Raises:
        ValueError: Raised when dataset validation fails.
    """

    # Historical extraction window aligned with weekly scheduling
    end_dt = datetime(2026, 4, 26, 23, 59, tzinfo=timezone.utc)
    start_dt = datetime(2025, 4, 27, 0, 0, tzinfo=timezone.utc)

    logger.info(
        "Building BTC historical dataset: start=%s end=%s",
        start_dt,
        end_dt,
    )

    # Extract OHLCV data using fallback strategy
    df = extract_with_fallback(
        start_dt=start_dt,
        end_dt=end_dt,
    )

    # Validate extracted dataset quality
    if not validate_ohlcv_dataframe(df, start_dt, end_dt):
        raise ValueError("Historical BTC dataset validation failed")

    # Save dataset locally as Parquet
    local_path = Path("data/btc_1m_history_1y.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        local_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    # Target GCS location
    gcs_path = (
        f"{settings.GCS_PREFIX}/history/"
        f"btc_1m_history_1y.parquet"
    )

    # Upload dataset to GCS
    gcs_uri = upload_to_gcs(
        local_path=str(local_path),
        gcs_path=gcs_path,
    )

    logger.info("Backfill uploaded to %s", gcs_uri)

    return gcs_uri


if __name__ == "__main__":
    build_btc_history_1y()