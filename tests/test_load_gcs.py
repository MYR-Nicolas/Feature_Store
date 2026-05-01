import pytest
from unittest.mock import MagicMock

from ELT.load import upload_to_gcs


def test_upload_file_to_gcs_success(mocker):
    """
    Test successful upload to GCS using mocked client.
    """

    mock_client = mocker.patch("ELT.load.storage.Client")

    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    mock_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    uri = upload_to_gcs(
        local_path="test.parquet",
        gcs_path="bronze/test.parquet",
        bucket_name="test-bucket"
    )

    assert uri == "gs://test-bucket/bronze/test.parquet"
    mock_blob.upload_from_filename.assert_called_once_with("test.parquet")


def test_upload_uses_default_bucket(mocker):
    """
    Test that default bucket from settings is used when none is provided.
    """

    mock_client = mocker.patch("ELT.load.storage.Client")
    mock_settings = mocker.patch("ELT.load.settings")

    mock_settings.GCS_BUCKET_NAME = "default-bucket"

    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    mock_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    uri = upload_to_gcs(
        local_path="file.parquet",
        gcs_path="path/file.parquet",
    )

    assert uri == "gs://default-bucket/path/file.parquet"