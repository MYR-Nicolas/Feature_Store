import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery


def _parse_run_results(path: Path, run_id: str) -> list[dict]:
    """Parse a dbt run_results.json and return normalized rows."""
    if not path or not path.exists():
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for result in data.get("results", []):
        node = result.get("unique_id", "")
        parts = node.split(".") if node else []
        resource_type = parts[0] if parts else None

        if resource_type == "test" and len(parts) >= 3:
            model_name = parts[-2]
        elif parts:
            model_name = parts[-1]
        else:
            model_name = None
        rows.append({
            "run_id": run_id,
            "resource_type": resource_type,
            "model_name": model_name,
            "status": result.get("status"),
            "execution_time": result.get("execution_time"),
            "message": result.get("message"),
            "compiled_code": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    return rows


def insert_dbt_results(
    project_id: str,
    run_id: str,
    run_results_path: Path,
    run_results_run_path: Path | None = None,
) -> None:
    # Merge models + tests
    rows = _parse_run_results(run_results_run_path, run_id) + _parse_run_results(run_results_path, run_id)

    if not rows:
        return

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.monitoring.dbt_results"

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery dbt_results insert failed: {errors}")