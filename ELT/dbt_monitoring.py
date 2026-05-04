import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery


def insert_dbt_results(
    project_id: str,
    run_id: str,
    run_results_path: Path,
) -> None:
    if not run_results_path.exists():
        return

    with open(run_results_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    rows = []

    for result in data.get("results", []):
        node = result.get("unique_id", "")
        model_name = node.split(".")[-1] if node else None

        rows.append(
            {
                "run_id": run_id,
                "resource_type": node.split(".")[0] if node else None,
                "model_name": model_name,
                "status": result.get("status"),
                "execution_time": result.get("execution_time"),
                "message": result.get("message"),
                "compiled_code": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    if not rows:
        return

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.monitoring.dbt_results"

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery dbt_results insert failed: {errors}")