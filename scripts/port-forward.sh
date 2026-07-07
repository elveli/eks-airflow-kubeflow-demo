#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Local access to both UIs — NO load balancers, NO public exposure, $0.
# Ctrl-C stops both forwards.
# -----------------------------------------------------------------------------
set -euo pipefail

echo "Airflow UI            → http://localhost:8080   (login: admin / admin)"
echo "Kubeflow Pipelines UI → http://localhost:8081"
echo "Ctrl-C to stop."
echo ""

trap 'kill 0' EXIT INT TERM
kubectl -n airflow port-forward svc/airflow-webserver 8080:8080 &
kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8081:80 &
wait
