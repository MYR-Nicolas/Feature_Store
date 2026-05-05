import logging
import os
import subprocess
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from ELT.extract import extract_with_fallback
from ELT.load import load_df_to_gcs
from ELT.config import settings
from ELT.monitoring import insert_pipeline_run
from ELT.dbt_monitoring import insert_dbt_results
from ELT.monitoring_data_quality import compute_quality_metrics, insert_quality_metrics
from ELT.export_metrics import export_monitoring_cache


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def run_command(command: list[str], cwd: Path) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        shell=False,
    )

    if result.stdout:
        logger.info(result.stdout)

    if result.stderr:
        logger.error(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(command)}")


def run_dbt_pipeline() -> None:
    logger.info("Running dbt debug")
    run_command(["dbt", "debug", "--profiles-dir", "."], DBT_DIR)

    logger.info("Installing dbt dependencies")
    run_command(["dbt", "deps", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt transformations")
    run_command(["dbt", "run", "--profiles-dir", "."], DBT_DIR)

    logger.info("Running dbt tests")
    run_command(["dbt", "test", "--profiles-dir", "."], DBT_DIR)


def build_weekly_paths() -> tuple[str, str]:
    execution_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    filename = f"btc_1m_weekly_{execution_date}.parquet"

    local_path = f"data/weekly/{filename}"
    gcs_path = f"{settings.GCS_PREFIX}/weekly/{filename}"

    return local_path, gcs_path


def run_pipeline():
    logger.info("Starting BTC weekly ELT pipeline")

    df = extract_with_fallback()

    rows_extracted = len(df)
    rows_loaded = len(df)

    logger.info("Extracted rows: %s", rows_extracted)
    logger.info("Columns: %s", list(df.columns))

    local_path, gcs_path = build_weekly_paths()

    gcs_uri = load_df_to_gcs(
        df=df,
        local_path=local_path,
        gcs_path=gcs_path,
    )

    logger.info("Uploaded weekly OHLCV data to: %s", gcs_uri)

    run_dbt_pipeline()

    return rows_extracted, rows_loaded, df


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]

    run_id = str(uuid.uuid4())
    github_sha = os.getenv("GITHUB_SHA")
    image_uri = os.getenv("IMAGE_URI")

    started_at = datetime.now(timezone.utc)
    start_time = time.time()

    status = "success"
    error_message = None
    rows_extracted = None
    rows_loaded = None
    df = None
    metrics = None

    try:
        rows_extracted, rows_loaded, df = run_pipeline()
        metrics = compute_quality_metrics(df)

    except Exception:
        status = "failed"
        error_message = traceback.format_exc()
        logger.exception("Pipeline failed")
        raise

    finally:
        ended_at = datetime.now(timezone.utc)
        duration_seconds = round(time.time() - start_time, 2)

        try:
            insert_pipeline_run(
                project_id=project_id,
                run_id=run_id,
                pipeline_name="btc-pipeline",
                status=status,
                started_at=started_at,
                ended_at=ended_at,
                duration_seconds=duration_seconds,
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                error_message=error_message[:1000] if error_message else None,
                github_sha=github_sha,
                image_uri=image_uri,
            )

            logger.info(
                "Pipeline run logged to BigQuery with status=%s, duration=%s seconds",
                status,
                duration_seconds,
            )

        except Exception:
            logger.exception("Failed to log pipeline run to BigQuery")

        try:
            insert_dbt_results(
                project_id=project_id,
                run_id=run_id,
                run_results_path=DBT_DIR / "target" / "run_results.json",
            )

            logger.info("dbt results logged to BigQuery")

        except Exception:
            logger.exception("Failed to log dbt results to BigQuery")

        if metrics is not None:
            try:
                insert_quality_metrics(
                    project_id=project_id,
                    run_id=run_id,
                    metrics=metrics,
                )

                logger.info("Data quality metrics logged to BigQuery")

            except Exception:
                logger.exception("Failed to log data quality metrics to BigQuery")

        # Export metrics GCS pour le dashboard Streamlit
        bucket_name = os.getenv("GCS_BUCKET_NAME", "")
        if bucket_name:
            try:
                export_monitoring_cache(
                    project_id=project_id,
                    bucket_name=bucket_name,
                    run_id=run_id,
                    pipeline_run={
                        "run_id": run_id,
                        "pipeline_name": "btc-pipeline",
                        "status": status,
                        "started_at": started_at.isoformat(),
                        "ended_at": ended_at.isoformat(),
                        "duration_seconds": duration_seconds,
                        "rows_extracted": rows_extracted,
                        "rows_loaded": rows_loaded,
                        "error_message": error_message[:1000] if error_message else None,
                    },
                    dbt_results=[],
                    quality_metrics=metrics or {},
                    dbt_run_results_path=DBT_DIR / "target" / "run_results.json",
                )
                logger.info("Monitoring cache exported to GCS")
            except Exception:
                logger.exception("Failed to export monitoring cache to GCS")


if __name__ == "__main__":
    main()