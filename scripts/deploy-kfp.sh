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

# 2.16.1: DON'T pin older versions casually — Google purged many old
# gcr.io/ml-pipeline tags when Container Registry was sunset, so e.g. the
# 2.2.0/2.5.0 manifests reference images that no longer exist (their minio
# tag 404s). 2.14+ pulls from ghcr.io / Docker Hub / quay.io instead and
# ships seaweedfs as the object store.
KFP_VERSION="${KFP_VERSION:-2.16.1}"
ROLE_ARN="$(terraform -chdir=terraform output -raw kfp_irsa_role_arn)"

echo ">>> Installing Kubeflow Pipelines standalone ${KFP_VERSION}"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${KFP_VERSION}"
kubectl wait --for=condition=established --timeout=120s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=${KFP_VERSION}"

echo ">>> Annotating pipeline-runner SA with IRSA role (S3 access for pipeline pods)"
kubectl -n kubeflow annotate serviceaccount pipeline-runner \
  "eks.amazonaws.com/role-arn=${ROLE_ARN}" --overwrite

# KFP's result-cache can't start on EKS: cache-deployer mints its webhook TLS
# cert through the Kubernetes CSR API, and EKS's signer refuses non-node
# certificates — the deployer crashloops and cache-server waits forever for a
# secret that never appears. Caching is optional (the sample DAG submits with
# enable_caching=False anyway), so run without it.
echo ">>> Disabling KFP result-cache (its CSR cert flow is incompatible with EKS)"
kubectl -n kubeflow scale deploy cache-deployer-deployment cache-server --replicas=0
kubectl delete csr cache-server.kubeflow --ignore-not-found

echo ">>> Waiting for ALL KFP deployments (first image pulls take several minutes)"
for d in $(kubectl -n kubeflow get deploy -o name); do
  kubectl -n kubeflow rollout status "$d" --timeout=900s
done

echo ""
echo ">>> Kubeflow Pipelines is ready."
echo "    UI: kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8081:80  →  http://localhost:8081"
