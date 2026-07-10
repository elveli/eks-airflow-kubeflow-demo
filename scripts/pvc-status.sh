#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make pvc`: every PVC joined to its bound volume's AZ, flagged STRANDED when
# no live node is in that AZ — the pod can never schedule there, because EBS
# volumes are AZ-bound (the mysql-Pending incident; see README: "If
# mysql/seaweedfs stay Pending…" for the delete-PVC-and-redeploy recovery).
#
# The k8s-side mirror of `make volumes` (which is the AWS billing view and
# works while the cluster is parked): this one shows binding + schedulability.
# -----------------------------------------------------------------------------
set -euo pipefail

NODE_AZS="$(kubectl get nodes -o jsonpath='{.items[*].metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null || true)"

{
  echo "CLAIM STATUS VOLUME AZ SCHEDULABLE"
  kubectl get pv -o json | jq -r --arg azs "$NODE_AZS" '
    ($azs | split(" ") | map(select(. != ""))) as $live
    | .items[]
    | (.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0] // "?") as $az
    | ((.spec.claimRef // {}) | ((.namespace // "?") + "/" + (.name // "?"))) as $claim
    | $claim + " " + .status.phase + " " + .metadata.name + " " + $az + " "
      + (if ($live | length) == 0 then "no-nodes"
         elif $az == "?" then "?"
         elif ($live | index($az)) then "yes"
         else "STRANDED" end)'
} | column -t

# Unbound claims have no PV yet, so the table above never shows them.
PENDING="$(kubectl get pvc -A --no-headers 2>/dev/null | awk '$3 != "Bound" {print "  " $1 "/" $2 " (" $3 ")"}')"
echo ""
echo "Unbound PVCs (WaitForFirstConsumer — normal briefly after PVC recreation):"
echo "${PENDING:-  none}"

echo ""
echo "(STRANDED = the volume's AZ has no live node, so its pod can't schedule;"
echo " recovery recipe in README under 'If mysql/seaweedfs stay Pending')"
