#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# COST KILL SWITCH — park the cluster without destroying anything.
#
#   ./scripts/kill-switch.sh off   → scale BOTH node groups to zero
#   ./scripts/kill-switch.sh on    → restore normal scaling
#
# While OFF you pay only:
#   * EKS control plane            ~$0.10/h
#   * EBS volumes (PVCs + nothing else, node disks die with the nodes) ~$0.005/h
#   * S3 storage                   pennies
# ≈ $2.50/day instead of ~$4/day. All state (Airflow DB, KFP MySQL/minio)
# survives on the PVCs; pods just sit Pending until you switch back on.
#
# NOTE: `terraform apply` after an 'off' will restore min_size (desired_size
# is ignored via lifecycle) — that's fine, apply is effectively 'on'.
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

TF="terraform -chdir=terraform"
REGION="$($TF output -raw region)"
CLUSTER="$($TF output -raw cluster_name)"
MODE="${1:-}"

scale() { # name min max desired
  aws eks update-nodegroup-config --region "$REGION" --cluster-name "$CLUSTER" \
    --nodegroup-name "$1" \
    --scaling-config "minSize=$2,maxSize=$3,desiredSize=$4" >/dev/null
  echo "  node group '$1' → min=$2 max=$3 desired=$4"
}

case "$MODE" in
  off)
    # Stop the autoscaler FIRST or it will immediately scale general back up
    # for the pending Airflow pods. (maxSize must be >= 1 per the EKS API.)
    echo ">>> Pausing cluster-autoscaler"
    kubectl -n kube-system scale deploy \
      -l app.kubernetes.io/name=aws-cluster-autoscaler --replicas=0 || true
    echo ">>> Scaling node groups to zero"
    scale general 0 1 0
    scale pipelines 0 1 0
    echo ">>> Parked. Burn rate ≈ \$0.105/h (control plane + PVC storage)."
    ;;
  on)
    echo ">>> Restoring node groups"
    scale general 1 2 1
    scale pipelines 0 2 0
    echo ">>> Resuming cluster-autoscaler"
    kubectl -n kube-system scale deploy \
      -l app.kubernetes.io/name=aws-cluster-autoscaler --replicas=1 || true
    echo ">>> Pods will reschedule over the next ~5 minutes."
    ;;
  *)
    echo "usage: $0 on|off" >&2
    exit 1
    ;;
esac
