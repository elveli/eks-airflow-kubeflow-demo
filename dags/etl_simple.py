"""Sample DAG 1 — a minimal ETL. Dummy edit here to trigger Airflow event. Dummy chg.

extract → transform → load, ending with a JSON summary written to
s3://$DEMO_S3_BUCKET/etl/<ds>/summary.json.

The S3 write uses plain boto3 with NO credentials configured anywhere:
the task pod's service account (airflow-worker) is IRSA-annotated, so the
default credential chain picks up the web-identity role automatically.
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("DEMO_S3_BUCKET", "")

CITIES = ["Helsinki", "Stockholm", "Copenhagen", "Oslo", "Reykjavik"]

# Data-aware scheduling: the `load` task declares it UPDATES this dataset
# (see outlets below). Airflow emits a dataset event on task success, which
# triggers any DAG scheduled on it — train_on_kubeflow, in this demo. The
# URI is an identifier, not a watched location: Airflow never polls S3.
ETL_SUMMARY_DATASET = Dataset(f"s3://{S3_BUCKET}/etl/summary")


@dag(
    dag_id="etl_simple",
    description="Minimal ETL demo: generate → aggregate → write to S3 via IRSA",
    schedule=None,  # trigger manually from the UI
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "etl"],
)
def etl_simple():
    @task
    def extract() -> list[dict]:
        """Pretend to pull order rows from an upstream system."""
        rng = random.Random(42)  # deterministic so reruns are comparable
        rows = [
            {
                "order_id": i,
                "city": rng.choice(CITIES),
                "amount_eur": round(rng.uniform(5, 500), 2),
            }
            for i in range(1000)
        ]
        log.info("Extracted %d rows", len(rows))
        return rows

    @task
    def transform(rows: list[dict]) -> dict:
        """Aggregate revenue per city."""
        revenue: dict[str, float] = {}
        for row in rows:
            revenue[row["city"]] = round(revenue.get(row["city"], 0.0) + row["amount_eur"], 2)
        summary = {
            "row_count": len(rows),
            "revenue_per_city": dict(sorted(revenue.items())),
            "total_revenue": round(sum(revenue.values()), 2),
        }
        log.info("Summary: %s", summary)
        return summary

    @task(outlets=[ETL_SUMMARY_DATASET])
    def load(summary: dict) -> str:
        """Write the summary to S3 (credentials come from IRSA)."""
        import boto3
        from airflow.operators.python import get_current_context

        if not S3_BUCKET:
            raise ValueError("DEMO_S3_BUCKET env var is not set")

        ds = get_current_context()["ds"]
        key = f"etl/{ds}/summary.json"
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(summary, indent=2).encode(),
            ContentType="application/json",
        )
        uri = f"s3://{S3_BUCKET}/{key}"
        log.info("Wrote %s", uri)
        return uri

    load(transform(extract()))


etl_simple()
