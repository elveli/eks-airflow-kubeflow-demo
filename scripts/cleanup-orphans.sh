#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Find (and optionally delete) resources that Kubernetes created OUTSIDE of
# Terraform's knowledge and that keep billing after `terraform destroy`:
#
#   * EBS volumes from PVCs (Airflow Postgres, KFP MySQL/minio)
#   * ALB/NLB/classic ELBs created by the LB controller or Service objects
#   * security groups the LB controller attached to those ELBs
#   * CloudWatch log groups (none are created by this repo's config, but
#     checked in case someone enabled control-plane logging)
#   * the S3 bucket (Terraform-managed with force_destroy — listed as a check)
#
# Default is a DRY RUN report. Pass --delete to actually remove what it finds.
# Works after destroy too: reads cluster/region from terraform output when
# available, else from CLUSTER_NAME / AWS_REGION env vars, else defaults.
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

DELETE=false
[ "${1:-}" = "--delete" ] && DELETE=true

TF="terraform -chdir=terraform"
CLUSTER="${CLUSTER_NAME:-$($TF output -raw cluster_name 2>/dev/null || echo afkf-demo-eks)}"
REGION="${AWS_REGION:-$($TF output -raw region 2>/dev/null || echo eu-north-1)}"
TAG="kubernetes.io/cluster/${CLUSTER}"
FOUND=0

echo "Cluster: ${CLUSTER}  Region: ${REGION}  Mode: $($DELETE && echo DELETE || echo dry-run)"
echo ""

# --- EBS volumes -------------------------------------------------------------
echo "== EBS volumes tagged ${TAG} =="
VOLS="$(aws ec2 describe-volumes --region "$REGION" \
  --filters "Name=tag:${TAG},Values=owned" \
  --query 'Volumes[?State==`available`].VolumeId' --output text | tr '\t' '\n' | grep -v '^$' || true)"
for v in $VOLS; do
  FOUND=1
  echo "  ORPHAN: $v"
  $DELETE && aws ec2 delete-volume --region "$REGION" --volume-id "$v" && echo "  deleted $v"
done

# --- ALB/NLB (elbv2) -----------------------------------------------------------
echo "== ALB/NLB load balancers tagged for the cluster =="
for arn in $(aws elbv2 describe-load-balancers --region "$REGION" \
    --query 'LoadBalancers[].LoadBalancerArn' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true); do
  match="$(aws elbv2 describe-tags --region "$REGION" --resource-arns "$arn" \
    --query "TagDescriptions[0].Tags[?(Key=='elbv2.k8s.aws/cluster' || Key=='${TAG}') && (Value=='${CLUSTER}' || Value=='owned')] | length(@)" \
    --output text)"
  if [ "$match" != "0" ]; then
    FOUND=1
    echo "  ORPHAN: $arn"
    $DELETE && aws elbv2 delete-load-balancer --region "$REGION" --load-balancer-arn "$arn" && echo "  deleted"
  fi
done

# --- Classic ELBs ---------------------------------------------------------------
echo "== Classic ELBs tagged for the cluster =="
for name in $(aws elb describe-load-balancers --region "$REGION" \
    --query 'LoadBalancerDescriptions[].LoadBalancerName' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true); do
  match="$(aws elb describe-tags --region "$REGION" --load-balancer-names "$name" \
    --query "TagDescriptions[0].Tags[?Key=='${TAG}'] | length(@)" --output text)"
  if [ "$match" != "0" ]; then
    FOUND=1
    echo "  ORPHAN: $name"
    $DELETE && aws elb delete-load-balancer --region "$REGION" --load-balancer-name "$name" && echo "  deleted"
  fi
done

# --- Security groups left behind by k8s-created ELBs ----------------------------
echo "== Security groups tagged for the cluster (outside Terraform) =="
SGS="$(aws ec2 describe-security-groups --region "$REGION" \
  --filters "Name=tag:${TAG},Values=owned" \
  --query 'SecurityGroups[].GroupId' --output text | tr '\t' '\n' | grep -v '^$' || true)"
for sg in $SGS; do
  FOUND=1
  echo "  ORPHAN: $sg"
  $DELETE && aws ec2 delete-security-group --region "$REGION" --group-id "$sg" && echo "  deleted $sg"
done

# --- CloudWatch log groups -------------------------------------------------------
echo "== CloudWatch log groups for the cluster =="
LGS="$(aws logs describe-log-groups --region "$REGION" \
  --log-group-name-prefix "/aws/eks/${CLUSTER}" \
  --query 'logGroups[].logGroupName' --output text | tr '\t' '\n' | grep -v '^$' || true)"
for lg in $LGS; do
  FOUND=1
  echo "  ORPHAN: $lg"
  $DELETE && aws logs delete-log-group --region "$REGION" --log-group-name "$lg" && echo "  deleted $lg"
done

# --- S3 (informational — terraform destroy removes it via force_destroy) ---------
echo "== S3 buckets with the project prefix =="
aws s3api list-buckets --query "Buckets[?starts_with(Name, 'afkf-demo-')].Name" --output text \
  | tr '\t' '\n' | grep -v '^$' | sed 's/^/  STILL EXISTS (destroy should have removed it): /' || echo "  none"

echo ""
if [ "$FOUND" = "0" ]; then
  echo "Nothing orphaned — you are not being billed for leftovers. ✓"
else
  $DELETE || echo "Orphans found. Re-run with --delete to remove them."
fi
