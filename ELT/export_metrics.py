"""
ELT/export_cache.py
Exporte les données de monitoring vers GCS après chaque run.
Le dashboard Streamlit lit ce fichier JSON via URL publique — sans auth GCP.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

GCS_CACHE_BLOB = "monitoring_cache/latest.json"


def export_monitoring_cache(
    project_id: str,
    bucket_name: str,
    run_id: str,
    pipeline_run: dict,
    dbt_results: list[dict],
    quality_metrics: dict,
    dbt_run_results_path: Path | None = None,
) -> str:
    """
    Construit le payload de monitoring et l'uploade sur GCS.
    Retourne l'URL publique du fichier.
    """
    from google.cloud import bigquery, storage

    # ── Récupère l'historique des 20 dernières runs depuis BQ ────────────────
    history = []
    try:
        bq = bigquery.Client(project=project_id)
        rows = list(bq.query(f"""
            SELECT run_id, status, started_at, duration_seconds,
                   rows_extracted, rows_loaded
            FROM `{project_id}.monitoring.pipeline_runs`
            ORDER BY started_at DESC
            LIMIT 20
        """).result())
        history = [dict(r) for r in rows]
    except Exception:
        logger.warning("Could not fetch pipeline history from BigQuery for cache export")

    # ── Récupère les résultats dbt depuis run_results.json ───────────────────
    dbt_rows = []
    if dbt_run_results_path and dbt_run_results_path.exists():
        try:
            with open(dbt_run_results_path, encoding="utf-8") as f:
                data = json.load(f)
            for result in data.get("results", []):
                node = result.get("unique_id", "")
                dbt_rows.append({
                    "run_id": run_id,
                    "resource_type": node.split(".")[0] if node else None,
                    "model_name": node.split(".")[-1] if node else None,
                    "status": result.get("status"),
                    "execution_time": result.get("execution_time"),
                    "message": result.get("message"),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
        except Exception:
            logger.warning("Could not parse dbt run_results.json for cache export")
    else:
        dbt_rows = dbt_results  # fallback si déjà parsé

    # ── Construit le payload complet ─────────────────────────────────────────
    fetched_at = datetime.now(timezone.utc).isoformat()

    payload = {
        "pipeline": {
            **pipeline_run,
            "history": history,
            "_fetched_at": fetched_at,
        },
        "dbt": {
            "rows": dbt_rows,
            "_fetched_at": fetched_at,
        },
        "quality": {
            "run_id": run_id,
            "metrics": quality_metrics,
            "created_at": fetched_at,
            "_fetched_at": fetched_at,
        },
        "_exported_at": fetched_at,
        "_run_id": run_id,
    }

    # ── Upload vers GCS ───────────────────────────────────────────────────────
    gcs = storage.Client(project=project_id)
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(GCS_CACHE_BLOB)

    blob.upload_from_string(
        json.dumps(payload, default=str, indent=2),
        content_type="application/json",
    )

    public_url = f"https://storage.googleapis.com/{bucket_name}/{GCS_CACHE_BLOB}"
    logger.info("Monitoring cache exported to GCS: %s", public_url)
    return public_url
