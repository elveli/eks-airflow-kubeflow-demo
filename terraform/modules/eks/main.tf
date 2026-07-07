# -----------------------------------------------------------------------------
# EKS cluster + two SPOT managed node groups.
#
# Cost decisions baked in here:
#   * upgrade_policy STANDARD  → never silently roll into $0.60/h extended support
#   * no control-plane logging → no CloudWatch log group, no ingestion charges
#   * capacity_type SPOT       → ~60-70% off on-demand
#   * pipelines group min=0    → scale-to-zero when no KFP run is active
# -----------------------------------------------------------------------------

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
    tls = { source = "hashicorp/tls" }
  }
}

# --- Cluster IAM role ---------------------------------------------------------
data "aws_iam_policy_document" "cluster_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster_name}-cluster"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster_policy" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

# --- Cluster ------------------------------------------------------------------
resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.cluster.arn
  version  = var.cluster_version

  access_config {
    authentication_mode = "API_AND_CONFIG_MAP"
    # Whoever runs `terraform apply` becomes cluster admin — exactly what a
    # single-operator demo wants.
    bootstrap_cluster_creator_admin_permissions = true
  }

  # COST: opt out of paid extended support. When 1.33 leaves standard support
  # AWS auto-upgrades the control plane instead of billing $0.60/h.
  upgrade_policy {
    support_type = "STANDARD"
  }

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_public_access  = true
    endpoint_private_access = true
    public_access_cidrs     = var.api_allowed_cidrs
  }

  # enabled_cluster_log_types intentionally NOT set: control-plane logs create
  # a CloudWatch log group that costs money and leaks after destroy.

  depends_on = [aws_iam_role_policy_attachment.cluster_policy]
}

# --- IRSA: OIDC provider for the cluster --------------------------------------
data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

# --- Node IAM role (shared by both groups) -------------------------------------
data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# --- Node groups ---------------------------------------------------------------
locals {
  node_groups = {
    # Airflow (scheduler/webserver/postgres), KFP control plane (mysql, minio,
    # api server, workflow controller) and cluster addons all run here.
    general = {
      instance_types = var.general_instance_types
      scaling        = var.general_scaling
      labels         = { workload = "general" }
      taints         = []
    }

    # KFP executor pods only. Scales 0 → N on demand, back to 0 ~2 min after
    # the run finishes (see autoscaler args in the addons module).
    pipelines = {
      instance_types = var.pipelines_instance_types
      scaling        = var.pipelines_scaling
      labels         = { workload = "pipelines" }
      taints = var.enable_pipelines_taint ? [{
        key    = "workload"
        value  = "pipelines"
        effect = "NO_SCHEDULE"
      }] : []
    }
  }
}

resource "aws_eks_node_group" "this" {
  for_each = local.node_groups

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  capacity_type  = "SPOT" # throwaway demo — interruptions are acceptable
  instance_types = each.value.instance_types
  ami_type       = "AL2023_x86_64_STANDARD"
  disk_size      = 20 # GiB, gp3 on AL2023 — the minimum sensible size

  scaling_config {
    min_size     = each.value.scaling.min_size
    max_size     = each.value.scaling.max_size
    desired_size = each.value.scaling.desired_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = each.value.labels

  dynamic "taint" {
    for_each = each.value.taints

    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  lifecycle {
    # The cluster autoscaler owns desired_size at runtime — don't fight it
    # on the next `terraform apply`.
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

# --- Cluster-autoscaler discovery tags -----------------------------------------
# Managed node groups do NOT propagate their tags to the underlying ASG, but
# the autoscaler discovers node groups by ASG tag — so tag the ASGs explicitly.
locals {
  asg_ca_tags = merge([
    for ng_name, _ in local.node_groups : {
      "${ng_name}/enabled" = {
        ng    = ng_name
        key   = "k8s.io/cluster-autoscaler/enabled"
        value = "true"
      }
      "${ng_name}/owned" = {
        ng    = ng_name
        key   = "k8s.io/cluster-autoscaler/${var.cluster_name}"
        value = "owned"
      }
    }
  ]...)
}

resource "aws_autoscaling_group_tag" "cluster_autoscaler" {
  for_each = local.asg_ca_tags

  autoscaling_group_name = aws_eks_node_group.this[each.value.ng].resources[0].autoscaling_groups[0].name

  tag {
    key                 = each.value.key
    value               = each.value.value
    propagate_at_launch = false
  }
}
