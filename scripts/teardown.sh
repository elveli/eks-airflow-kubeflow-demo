#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# One-command teardown, in the RIGHT ORDER.
#
# Deleting the app namespaces BEFORE `terraform destroy` is what prevents the
# two classic billing leaks:
#   * EBS volumes: PVC deletion makes the CSI driver delete the backing
#     volumes. Kill the cluster first and those volumes are orphaned forever.
#   * ELBs/ALBs: anything created by the LB controller must be deleted by it,
#     while it is still running.
# Afterwards cleanup-orphans.sh double-checks nothing is left billing.
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

TF="terraform -chdir=terraform"
REGION="$($TF output -raw region 2>/dev/null || true)"
CLUSTER="$($TF output -raw cluster_name 2>/dev/null || true)"

if [ -n "$CLUSTER" ]; then
  aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER" >/dev/null 2>&1 || true
fi

if kubectl cluster-info >/dev/null 2>&1; then
  echo ">>> Deleting any LoadBalancer services (ELB leak prevention)"
  kubectl get svc -A -o json \
    | jq -r '.items[] | select(.spec.type=="LoadBalancer") | "\(.metadata.namespace) \(.metadata.name)"' \
    | while read -r ns name; do
        [ -n "$ns" ] && kubectl -n "$ns" delete svc "$name" --wait=true
      done

  echo ">>> Deleting app namespaces (lets the CSI driver delete PVC-backed EBS volumes)"
  kubectl delete namespace kubeflow airflow --ignore-not-found --timeout=600s || true

  echo ">>> Waiting for PersistentVolumes to be reclaimed"
  for _ in $(seq 1 30); do
    PV_COUNT="$(kubectl get pv --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    [ "$PV_COUNT" = "0" ] && break
    echo "    $PV_COUNT PV(s) remaining..."
    sleep 10
  done
else
  echo "WARN: cluster unreachable — skipping in-cluster cleanup."
  echo "      EBS volumes from PVCs may leak; run scripts/cleanup-orphans.sh --delete afterwards."
fi

echo ">>> terraform destroy"
$TF destroy -auto-approve

echo ">>> Checking for orphaned billable resources"
CLUSTER_NAME="${CLUSTER}" AWS_REGION="${REGION}" ./scripts/cleanup-orphans.sh || true

echo ""
echo ">>> Teardown complete. Local terraform state kept (harmless, costs nothing)."
