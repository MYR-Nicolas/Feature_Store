import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from ELT.extract import extract_with_fallback
from ELT.load import load_df_to_gcs
from ELT.config import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def run_command(command: list[str], cwd: Path) -> None:
    """
    Execute a shell command in a specified working directory.

    Args:
        command (list[str]): Command and arguments to execute.
        cwd (Path): Working directory where the command will be executed.

    Raises:
        RuntimeError: If the command exits with a non zero return code.
    """
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        shell=False,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")


def run_dbt_pipeline() -> None:
    """
    Execute the full dbt pipeline.

    Raises:
        RuntimeError: If any dbt command fails.
    """
    logger.info("Running dbt debug ")
    run_command(["dbt", "debug", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt transformations...")
    run_command(["dbt", "run", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt tests ")
    run_command(["dbt", "test", "--profiles-dir", "."], DBT_DIR)


def build_weekly_paths() -> tuple[str, str]:
    """
    Build local and GCS paths for the weekly BTC Parquet file.

    Returns:
        tuple[str, str]: Local Parquet path and GCS destination path.
    """
    execution_date = datetime.utcnow().strftime("%Y%m%d")

    filename = f"btc_1m_weekly_{execution_date}.parquet"

    local_path = f"data/weekly/{filename}"
    gcs_path = f"{settings.GCS_PREFIX}/weekly/{filename}"

    return local_path, gcs_path


def main() -> None:
    """
    Main entry point for the weekly BTC ELT pipeline.

    """
    start = time.time()

    logger.info("Starting BTC weekly ELT pipeline")

    df = extract_with_fallback()

    logger.info("Extracted rows: %s", len(df))
    logger.info("Columns: %s", list(df.columns))

    local_path, gcs_path = build_weekly_paths()

    gcs_uri = load_df_to_gcs(
        df=df,
        local_path=local_path,
        gcs_path=gcs_path,
    )

    logger.info("Uploaded weekly OHLCV data to: %s", gcs_uri)

    run_dbt_pipeline()

    duration = round((time.time() - start) * 1000)

    logger.info("Pipeline completed successfully in %s ms", duration)


if __name__ == "__main__":
    main()