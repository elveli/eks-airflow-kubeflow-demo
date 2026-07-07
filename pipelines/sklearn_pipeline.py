"""Kubeflow Pipelines v2 demo pipeline: train + evaluate a scikit-learn model.

    train ── model + held-out test set ──▶ evaluate_and_publish ──▶ s3://<bucket>/kfp-artifacts/...

Both components:
  * run on the 'pipelines' node group (node selector + toleration added via
    kfp-kubernetes) — the cluster autoscaler scales it up from ZERO for the
    run and back down ~2 minutes after it finishes;
  * upload to S3 with plain boto3 and no credentials: the pipeline-runner
    service account is IRSA-annotated by scripts/deploy-kfp.sh.

Compile (writes sklearn_pipeline.yaml next to this file):

    pip install -r requirements.txt
    python sklearn_pipeline.py

The compiled YAML is committed so Airflow can submit it without anyone
needing the KFP SDK locally.

NOTE: no `from __future__ import annotations` in this file — PEP 563 string
annotations break the KFP component type inspector.
"""

import pathlib

from kfp import compiler, dsl, kubernetes
from kfp.dsl import Dataset, Input, Metrics, Model, Output

BASE_IMAGE = "python:3.11-slim"


@dsl.component(
    base_image=BASE_IMAGE,
    packages_to_install=["scikit-learn==1.4.2", "joblib==1.4.2"],
)
def train(
    n_estimators: int,
    model: Output[Model],
    test_data: Output[Dataset],
):
    """Train a RandomForest on iris; emit the model and a held-out test set."""
    import json

    import joblib
    from sklearn.datasets import load_iris
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split

    X, y = load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    clf = RandomForestClassifier(n_estimators=n_estimators, random_state=42)
    clf.fit(X_train, y_train)
    print(f"Trained RandomForest({n_estimators=}) on {len(X_train)} samples")

    joblib.dump(clf, model.path)
    with open(test_data.path, "w") as f:
        json.dump({"X": X_test.tolist(), "y": y_test.tolist()}, f)


@dsl.component(
    base_image=BASE_IMAGE,
    packages_to_install=["scikit-learn==1.4.2", "joblib==1.4.2", "boto3==1.34.100"],
)
def evaluate_and_publish(
    model: Input[Model],
    test_data: Input[Dataset],
    s3_bucket: str,
    s3_prefix: str,
    accuracy_threshold: float,
    metrics: Output[Metrics],
) -> str:
    """Score the model on held-out data; publish to S3 if it clears the bar."""
    import json
    from datetime import datetime, timezone

    import boto3
    import joblib
    from sklearn.metrics import accuracy_score

    clf = joblib.load(model.path)
    with open(test_data.path) as f:
        data = json.load(f)

    accuracy = float(accuracy_score(data["y"], clf.predict(data["X"])))
    metrics.log_metric("accuracy", accuracy)
    print(f"Held-out accuracy: {accuracy:.4f} (threshold {accuracy_threshold})")

    if accuracy < accuracy_threshold:
        raise ValueError(f"Model rejected: accuracy {accuracy:.4f} < {accuracy_threshold}")

    # Credentials come from IRSA on the pipeline-runner service account.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    s3 = boto3.client("s3")
    model_key = f"{s3_prefix}/{stamp}/model.joblib"
    s3.upload_file(model.path, s3_bucket, model_key)
    s3.put_object(
        Bucket=s3_bucket,
        Key=f"{s3_prefix}/{stamp}/metrics.json",
        Body=json.dumps({"accuracy": accuracy, "published_at": stamp}).encode(),
        ContentType="application/json",
    )

    model_uri = f"s3://{s3_bucket}/{model_key}"
    print(f"Published {model_uri}")
    return model_uri


@dsl.pipeline(
    name="sklearn-iris-demo",
    description="Train/evaluate a RandomForest on iris and publish the model to S3",
)
def sklearn_iris_pipeline(
    s3_bucket: str,
    s3_prefix: str = "kfp-artifacts",
    n_estimators: int = 150,
    accuracy_threshold: float = 0.85,
):
    train_task = train(n_estimators=n_estimators)
    eval_task = evaluate_and_publish(
        model=train_task.outputs["model"],
        test_data=train_task.outputs["test_data"],
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        accuracy_threshold=accuracy_threshold,
    )

    for task in (train_task, eval_task):
        # Big enough requests that the pods can't squeeze onto the general
        # nodes even if the selector were removed — we want the autoscaler
        # to bring up a t3.xlarge from zero.
        task.set_cpu_request("1").set_memory_request("2Gi")
        # Pin to the scale-from-zero 'pipelines' node group...
        kubernetes.add_node_selector(task, label_key="workload", label_value="pipelines")
        # ...and tolerate its taint (drop this line + the taint via
        # enable_pipelines_taint=false in Terraform if needed).
        kubernetes.add_toleration(
            task, key="workload", operator="Equal", value="pipelines", effect="NoSchedule"
        )


if __name__ == "__main__":
    out = pathlib.Path(__file__).parent / "sklearn_pipeline.yaml"
    compiler.Compiler().compile(sklearn_iris_pipeline, package_path=str(out))
    print(f"Compiled → {out}")
