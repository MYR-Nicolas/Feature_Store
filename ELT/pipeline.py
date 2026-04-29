import logging
import os
import subprocess
import time
from pathlib import Path

from ELT.extract import extract_with_fallback
from ELT.load import load_raw_ohlcv


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

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")


def run_dbt_pipeline() -> None:
    """
    Execute the full dbt pipeline (debug, run, test).

    Raises:
        RuntimeError: If any dbt command fails.
    """
    logger.info("Running dbt debug...")
    run_command(["dbt", "debug", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt transformations...")
    run_command(["dbt", "run", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt tests...")
    run_command(["dbt", "test", "--profiles-dir", "."], DBT_DIR)


def main() -> None:
    """
    Main entry point for the ELT pipeline.

    This pipeline performs:
        Data extraction with fallback mechanism
        Loading raw OHLCV data into the database
        Running dbt transformations and tests

    Raises:
        RuntimeError: If any step in the pipeline fails.
    """
    start = time.time()

    logger.info("Starting BTC ELT pipeline")

    df = extract_with_fallback()

    logger.info("Extracted rows: %s", len(df))

    rows_loaded = load_raw_ohlcv(df)

    logger.info("Loaded rows into raw.ohlcv: %s", rows_loaded)

    run_dbt_pipeline()

    duration = round((time.time() - start) * 1000)

    logger.info("Pipeline completed successfully in %s ms", duration)


if __name__ == "__main__":
    main()