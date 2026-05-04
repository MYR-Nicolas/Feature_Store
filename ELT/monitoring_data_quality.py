from datetime import datetime, timezone
import pandas as pd
from google.cloud import bigquery


def compute_quality_metrics(df):
    metrics = {}

    ts_col = "timestamp" if "timestamp" in df.columns else "open_time"

    metrics["row_count"] = len(df)
    metrics["column_count"] = len(df.columns)
    metrics["null_count"] = int(df.isna().sum().sum())
    metrics["duplicate_count"] = int(df.duplicated().sum())

    ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

    metrics["min_timestamp"] = ts.min().timestamp()
    metrics["max_timestamp"] = ts.max().timestamp()

    return metrics


def insert_quality_metrics(project_id, run_id, metrics):
    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.monitoring.data_quality"

    rows = [
        {
            "run_id": run_id,
            "metric_name": k,
            "metric_value": float(v),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        for k, v in metrics.items()
    ]

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"Quality insert failed: {errors}")