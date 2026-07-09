#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make git-sync`: answers "did my DAG change reach Airflow yet?"
#
# Compares three commits:
#   local   — HEAD of this working copy
#   github  — HEAD of origin/main (what git-sync pulls from)
#   cluster — what the scheduler's git-sync sidecar has actually delivered
#
# The cluster rev is read inside the git-sync container, whose clone lives at
# $GITSYNC_ROOT/repo (the chart wires /git/repo → mounted into Airflow
# containers as /opt/airflow/dags/repo).
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

LOCAL="$(git rev-parse HEAD)"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
GITHUB="$(git ls-remote origin "refs/heads/${BRANCH}" | awk '{print $1}')"
CLUSTER="$(kubectl -n airflow exec deploy/airflow-scheduler -c git-sync -- \
  git -C /git/repo rev-parse HEAD 2>/dev/null || true)"

short() { echo "${1:-\?}" | cut -c1-9; }

echo "local  ($BRANCH):  $(short "$LOCAL")"
echo "github (origin):  $(short "$GITHUB")"
if [ -n "$CLUSTER" ]; then
  echo "cluster (synced): $(short "$CLUSTER")"
else
  echo "cluster (synced): unreachable"
fi
echo ""

if [ -z "$CLUSTER" ]; then
  echo "✗ cluster unreachable (not deployed / parked / wrong kube context — try: make kubeconfig)"
elif [ "$LOCAL" != "$GITHUB" ]; then
  if git merge-base --is-ancestor "$GITHUB" "$LOCAL" 2>/dev/null; then
    echo "✗ local is AHEAD of GitHub — 'git push' first; git-sync can only see pushed commits"
  else
    echo "✗ local differs from GitHub (behind or diverged) — 'git pull' to see what the cluster sees"
  fi
elif [ "$GITHUB" != "$CLUSTER" ]; then
  echo "… GitHub is ahead of the cluster — git-sync polls every 60s, wait a moment and re-run"
else
  echo "✓ in sync: the cluster is running exactly what you see locally"
fi