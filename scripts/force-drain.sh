#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make force-drain`: unstick node drains blocked by PodDisruptionBudgets.
#
# Deletes every non-DaemonSet pod on cordoned (SchedulingDisabled) nodes.
# Direct deletion bypasses PDBs — they only gate the eviction API — so the
# drain can finish and the node terminate. Replacement pods are created by
# the owning Deployments/StatefulSets and either reschedule (normal drain)
# or sit Pending (scale-to-zero shutdown). Data is untouched: PVCs detach
# cleanly and re-attach wherever the replacement lands.
#
# Demo-grade tool: in production you'd respect PDBs, not blanket-bypass them.
# -----------------------------------------------------------------------------
set -euo pipefail

CORDONED="$(kubectl get nodes --no-headers 2>/dev/null | awk '/SchedulingDisabled/{print $1}')"

if [ -z "$CORDONED" ]; then
  echo "No draining (cordoned) nodes — nothing to do."
  exit 0
fi

for node in $CORDONED; do
  echo ">>> unsticking $node"
  kubectl get pods -A --field-selector "spec.nodeName=$node" -o json \
    | jq -r '.items[]
        | select(([.metadata.ownerReferences[]? | select(.kind == "DaemonSet")] | length) == 0)
        | select(.metadata.deletionTimestamp == null)
        | .metadata.namespace + " " + .metadata.name' \
    | while read -r ns name; do
        [ -n "$ns" ] && kubectl -n "$ns" delete pod "$name" --wait=false
      done
done

echo ">>> Done. Nodes should finish draining and terminate within ~2 minutes."
echo ">>> Watch with: make pdbs   (and: make nodegroups)"
