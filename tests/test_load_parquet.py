import pandas as pd
from pathlib import Path

from ELT.load import save_to_parquet


def test_save_parquet_creates_file(tmp_path):
    """
    Test that a DataFrame is correctly saved as a Parquet file.
    """

    df = pd.DataFrame({
        "open": [100000, 101000],
        "close": [102000, 103000],
    })

    file_path = tmp_path / "test.parquet"

    result_path = save_to_parquet(df, str(file_path))

    assert Path(result_path).exists()


def test_save_parquet_preserves_data(tmp_path):
    """
    Test that saved Parquet file preserves DataFrame content.
    """

    df = pd.DataFrame({
        "open": [100000, 101000],
        "close": [102000, 103000],
    })

    file_path = tmp_path / "test.parquet"

    save_to_parquet(df, str(file_path))

    df_loaded = pd.read_parquet(file_path)

    pd.testing.assert_frame_equal(df, df_loaded)