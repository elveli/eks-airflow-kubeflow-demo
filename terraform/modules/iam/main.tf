# -----------------------------------------------------------------------------
# IRSA (IAM Roles for Service Accounts) — one role per Kubernetes service
# account that needs AWS API access. No node-level AWS permissions beyond the
# EKS-required managed policies; workloads get exactly what they need.
#
#   airflow            → S3 rw (remote task logs + ETL output)
#   kfp                → S3 rw (model artifacts from pipeline components)
#   cluster-autoscaler → ASG scaling + EKS DescribeNodegroup (scale-from-zero)
#   alb-controller     → official upstream policy (only used if you add an ALB)
#   ebs-csi            → AWS managed AmazonEBSCSIDriverPolicy
# -----------------------------------------------------------------------------

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

locals {
  irsa_roles = {
    airflow = {
      # The chart creates airflow-worker / airflow-scheduler / airflow-webserver
      # etc. — one wildcard trust condition covers them all.
      sa_subjects    = ["system:serviceaccount:airflow:airflow-*"]
      condition_test = "StringLike"
    }
    kfp = {
      # KFP standalone runs pipeline pods under this SA in the kubeflow ns.
      sa_subjects    = ["system:serviceaccount:kubeflow:pipeline-runner"]
      condition_test = "StringEquals"
    }
    cluster-autoscaler = {
      sa_subjects    = ["system:serviceaccount:kube-system:cluster-autoscaler"]
      condition_test = "StringEquals"
    }
    alb-controller = {
      sa_subjects    = ["system:serviceaccount:kube-system:aws-load-balancer-controller"]
      condition_test = "StringEquals"
    }
    ebs-csi = {
      sa_subjects    = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
      condition_test = "StringEquals"
    }
  }
}

data "aws_iam_policy_document" "trust" {
  for_each = local.irsa_roles

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = each.value.condition_test
      variable = "${var.oidc_issuer}:sub"
      values   = each.value.sa_subjects
    }
  }
}

resource "aws_iam_role" "irsa" {
  for_each = local.irsa_roles

  name               = "${var.cluster_name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.trust[each.key].json
}

# --- S3 read/write, scoped to the demo bucket only ----------------------------
data "aws_iam_policy_document" "s3_rw" {
  statement {
    sid       = "BucketLevel"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.s3_bucket_arn]
  }

  statement {
    sid       = "ObjectLevel"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.s3_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "airflow_s3" {
  name   = "s3-rw"
  role   = aws_iam_role.irsa["airflow"].id
  policy = data.aws_iam_policy_document.s3_rw.json
}

resource "aws_iam_role_policy" "kfp_s3" {
  name   = "s3-rw"
  role   = aws_iam_role.irsa["kfp"].id
  policy = data.aws_iam_policy_document.s3_rw.json
}

# --- Cluster autoscaler --------------------------------------------------------
data "aws_iam_policy_document" "cluster_autoscaler" {
  statement {
    sid = "ReadOnly"
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeScalingActivities",
      "autoscaling:DescribeTags",
      "ec2:DescribeImages",
      "ec2:DescribeInstanceTypes",
      "ec2:DescribeLaunchTemplateVersions",
      "ec2:GetInstanceTypesFromInstanceRequirements",
      # Required for scale-FROM-zero on managed node groups: the autoscaler
      # reads labels/taints from the EKS API when no node exists to inspect.
      "eks:DescribeNodegroup",
    ]
    resources = ["*"]
  }

  statement {
    sid = "Scale"
    actions = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
    ]
    # Could be conditioned on the cluster discovery tag; kept broad for the demo.
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  name   = "cluster-autoscaler"
  role   = aws_iam_role.irsa["cluster-autoscaler"].id
  policy = data.aws_iam_policy_document.cluster_autoscaler.json
}

# --- AWS Load Balancer Controller ----------------------------------------------
# Vendored copy of the official policy:
# https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/main/docs/install/iam_policy.json
resource "aws_iam_role_policy" "alb_controller" {
  name   = "alb-controller"
  role   = aws_iam_role.irsa["alb-controller"].id
  policy = file("${path.module}/policies/alb-controller-iam-policy.json")
}

# --- EBS CSI driver -------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.irsa["ebs-csi"].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
