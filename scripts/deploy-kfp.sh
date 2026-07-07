#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Install Kubeflow Pipelines STANDALONE (not the full Kubeflow platform — far
# too heavy for two t3.larges) into the 'kubeflow' namespace.
#
# Why a script and not Terraform: upstream ships KFP as kustomize manifests
# only (no official Helm chart), and kustomize-via-null_resource is brittle,
# especially on destroy. This script is idempotent — re-run it freely.
#
# Requires: kubectl pointed at the cluster (make kubeconfig), git, terraform
# state present (for the IRSA role ARN output).
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

KFP_VERSION="${KFP_VERSION:-2.2.0}"
ROLE_ARN="$(terraform -chdir=terraform output -raw kfp_irsa_role_arn)"

echo ">>> Installing Kubeflow Pipelines standalone ${KFP_VERSION}"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${KFP_VERSION}"
kubectl wait --for=condition=established --timeout=120s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=${KFP_VERSION}"

echo ">>> Annotating pipeline-runner SA with IRSA role (S3 access for pipeline pods)"
kubectl -n kubeflow annotate serviceaccount pipeline-runner \
  "eks.amazonaws.com/role-arn=${ROLE_ARN}" --overwrite

echo ">>> Waiting for KFP deployments (first image pulls take several minutes)"
for d in ml-pipeline ml-pipeline-ui mysql minio workflow-controller metadata-grpc-deployment; do
  kubectl -n kubeflow rollout status "deploy/${d}" --timeout=900s
done

echo ""
echo ">>> Kubeflow Pipelines is ready."
echo "    UI: kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8081:80  →  http://localhost:8081"
