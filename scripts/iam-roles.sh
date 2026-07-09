#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# `make iam`: every IAM role this project created (they all share the cluster
# name as prefix), with who may assume it and which policies it carries.
#
# The AWS-side mirror of `make irsa`: irsa shows which service accounts CLAIM
# a role (the k8s annotation); this shows which principals each role TRUSTS
# (service accounts for IRSA roles, AWS services for cluster/node roles) —
# both must match for access to work.
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

# Belt and braces on the prefix: an EMPTY prefix would match every role in
# the account (observed after teardown, when terraform output returns empty
# without a nonzero exit).
CLUSTER="${CLUSTER_NAME:-$(terraform -chdir=terraform output -raw cluster_name 2>/dev/null || true)}"
CLUSTER="${CLUSTER:-afkf-demo-eks}"

{
  echo "ROLE TRUSTED_BY POLICIES CREATED"
  aws iam list-roles --query "Roles[?starts_with(RoleName, \`${CLUSTER}\`)].RoleName" --output text \
    | tr '\t' '\n' | while read -r role; do
      [ -z "$role" ] && continue
      doc="$(aws iam get-role --role-name "$role" --query 'Role.[CreateDate,AssumeRolePolicyDocument]' --output json)"
      created="$(echo "$doc" | jq -r '.[0]')"
      # Trusted principal: AWS service (cluster/node roles) or the
      # service-account subject condition (IRSA roles).
      trusted="$(echo "$doc" | jq -r '[.[1].Statement[]
        | (.Principal.Service? // empty),
          (.Condition? // {} | .[]? | to_entries[]? | select(.key | endswith(":sub")) | .value)]
        | flatten | join(",")')"
      managed="$(aws iam list-attached-role-policies --role-name "$role" \
        --query 'AttachedPolicies[].PolicyName' --output text | tr '\t' ',')"
      inline="$(aws iam list-role-policies --role-name "$role" \
        --query 'PolicyNames' --output text | tr '\t' ',')"
      policies="$(printf '%s,%s' "$managed" "$inline" | sed -E 's/^,+//; s/,+$//; s/,,+/,/g')"
      echo "$role ${trusted:--} ${policies:--} $created"
    done
} | sed -E 's/(T[0-9]{2}:[0-9]{2})[^[:space:]]*/\1/g' | column -t

echo ""
echo "(no rows = nothing deployed, or set CLUSTER_NAME=<name> if terraform state is gone)"
