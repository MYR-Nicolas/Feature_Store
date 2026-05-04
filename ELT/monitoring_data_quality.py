from datetime import datetime, timezone
from google.cloud import bigquery


def compute_quality_metrics(df):
    metrics = {}

    metrics["row_count"] = len(df)
    metrics["null_rate"] = df.isna().mean().mean()
    metrics["duplicate_rate"] = df.duplicated().mean()

    metrics["min_timestamp"] = df["timestamp"].min().timestamp()
    metrics["max_timestamp"] = df["timestamp"].max().timestamp()

    # gaps (important pour OHLCV)
    df_sorted = df.sort_values("timestamp")
    diffs = df_sorted["timestamp"].diff().dt.total_seconds()

    metrics["missing_intervals"] = (diffs > 60).sum()

    # anomalies simples
    metrics["price_spike"] = (df["close"].pct_change().abs() > 0.1).sum()
    metrics["volume_spike"] = (df["volume"].pct_change().abs() > 5).sum()

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