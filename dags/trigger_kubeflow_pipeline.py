"""Sample DAG 2 — Airflow orchestrating a Kubeflow Pipelines run end-to-end. Dummy edit here to trigger Airflow event.

submit_kfp_run:
    Runs in a per-task virtualenv (KubernetesExecutor pod pip-installs the
    KFP SDK at runtime — no custom Airflow image needed). It submits the
    pre-compiled pipeline package (pipelines/sklearn_pipeline.yaml, shipped
    into the pod by the same git-sync clone that delivers this DAG) to the
    in-cluster KFP API and blocks until the run finishes.

report_artifacts:
    Lists the model artifacts the pipeline published to S3, proving the
    whole chain worked: Airflow → KFP API → executor pods on the
    scale-from-zero 'pipelines' node group → S3 via IRSA.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

KFP_HOST = os.environ.get("DEMO_KFP_HOST", "http://ml-pipeline.kubeflow.svc.cluster.local:8888")
S3_BUCKET = os.environ.get("DEMO_S3_BUCKET", "")

# git-sync clones the whole repo; DAGs live in <clone>/dags, the compiled
# pipeline next door in <clone>/pipelines.
PIPELINE_PACKAGE = str(Path(__file__).resolve().parent.parent / "pipelines" / "sklearn_pipeline.yaml")


@dag(
    dag_id="train_on_kubeflow",
    description="Submit the scikit-learn KFP pipeline and wait for completion",
    schedule=None,  # trigger manually from the UI
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "kubeflow", "ml"],
)
def train_on_kubeflow():
    @task.virtualenv(
        requirements=["kfp==2.7.0"],
        system_site_packages=False,
        execution_timeout=timedelta(minutes=75),
    )
    def submit_kfp_run(host: str, package_path: str, s3_bucket: str) -> str:
        """Submit the compiled pipeline and wait for it to succeed.

        Self-contained on purpose: it executes inside a fresh virtualenv, so
        all imports happen in here.
        """
        import os
        import time

        from kfp.client import Client

        if not s3_bucket:
            raise ValueError("DEMO_S3_BUCKET env var is not set")
        if not os.path.exists(package_path):
            raise FileNotFoundError(
                f"{package_path} not found — compile it with `make pipeline` "
                "and push it to the DAGs repo (see README)."
            )

        client = Client(host=host)  # standalone KFP: no auth
        run = client.create_run_from_pipeline_package(
            pipeline_file=package_path,
            arguments={"s3_bucket": s3_bucket},
            run_name=f"airflow-triggered-{int(time.time())}",
            enable_caching=False,
        )
        print(f"Submitted KFP run {run.run_id}, waiting (first run scales a node from zero, ~5-10 min)")

        result = client.wait_for_run_completion(run.run_id, timeout=4000, sleep_duration=20)
        state = str(result.state)
        print(f"KFP run {run.run_id} finished with state: {state}")
        if state.upper() != "SUCCEEDED":
            raise RuntimeError(f"KFP run {run.run_id} ended in state {state}")
        return run.run_id

    @task
    def report_artifacts(kfp_run_id: str) -> list[str]:
        """List what the pipeline published to S3.

        NB: the parameter must NOT be called `run_id` — that's a reserved
        Airflow context key, and TaskFlow refuses arguments shadowing it.
        """
        import boto3

        resp = boto3.client("s3").list_objects_v2(Bucket=S3_BUCKET, Prefix="kfp-artifacts/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        log.info("KFP run %s artifacts in s3://%s:", kfp_run_id, S3_BUCKET)
        for key in keys:
            log.info("  %s", key)
        if not keys:
            raise RuntimeError("Pipeline succeeded but no artifacts found under kfp-artifacts/")
        return keys

    report_artifacts(submit_kfp_run(KFP_HOST, PIPELINE_PACKAGE, S3_BUCKET))


train_on_kubeflow()
