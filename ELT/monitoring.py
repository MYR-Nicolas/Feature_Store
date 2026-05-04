from datetime import datetime
from google.cloud import bigquery


def insert_pipeline_run(
    project_id: str,
    run_id: str,
    pipeline_name: str,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    duration_seconds: float,
    rows_extracted: int | None = None,
    rows_loaded: int | None = None,
    error_message: str | None = None,
    github_sha: str | None = None,
    image_uri: str | None = None,
):
    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.monitoring.pipeline_runs"

    row = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "status": status,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": float(duration_seconds),
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "error_message": error_message,
        "github_sha": github_sha,
        "image_uri": image_uri,
    }

    errors = client.insert_rows_json(table_id, [row])

    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")