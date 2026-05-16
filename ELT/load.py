import logging
from pathlib import Path

import pandas as pd
from google.cloud import storage

from ELT.config import settings

logger = logging.getLogger(__name__)


def save_to_parquet(
    df: pd.DataFrame,
    local_path: str,
) -> str:
    """
    Save a DataFrame to a local Parquet file.

    Args:
        df: DataFrame to save.
        local_path: Local file path for the Parquet file.

    Returns:
        Path to the saved Parquet file.
    """
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        local_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    logger.info("Parquet file saved locally: %s", local_path)

    return str(local_path)


def upload_to_gcs(
    local_path: str,
    gcs_path: str,
    bucket_name: str | None = None,
) -> str:
    """
    Upload a local file to Google Cloud Storage.

    Args:
        local_path: Path to the local file.
        gcs_path: Destination path inside the GCS bucket.
        bucket_name: Optional GCS bucket name.

    Returns:
        Full GCS URI of the uploaded file.
    """
    bucket_name = bucket_name or settings.GCS_BUCKET_NAME

    client = storage.Client(project=settings.GCP_PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_path)

    gcs_uri = f"gs://{bucket_name}/{gcs_path}"

    logger.info("Uploaded to GCS: %s", gcs_uri)

    return gcs_uri


def load_df_to_gcs(
    df: pd.DataFrame,
    local_path: str,
    gcs_path: str,
    bucket_name: str | None = None,
) -> str:
    """
    Save a DataFrame as Parquet and upload it to GCS.

    Args:
        df: DataFrame to save and upload.
        local_path: Local temporary Parquet file path.
        gcs_path: Destination path inside the GCS bucket.
        bucket_name: Optional GCS bucket name.

    Returns:
        GCS URI of the uploaded file.
    """
    local_file = save_to_parquet(df, local_path)

    return upload_to_gcs(local_file, gcs_path, bucket_name)