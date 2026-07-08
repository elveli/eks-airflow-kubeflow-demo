# -----------------------------------------------------------------------------
# In-cluster addons + the Airflow deployment itself.
#
#   * EBS CSI driver (EKS managed addon, IRSA)        — PVC support
#   * gp3 StorageClass as cluster default             — cheaper than gp2
#   * metrics-server                                  — `kubectl top`, tiny
#   * AWS Load Balancer Controller (1 replica, IRSA)  — idle unless you add an ALB
#   * cluster-autoscaler (IRSA, aggressive scale-down)
#   * Apache Airflow via the official Helm chart
#
# Kubeflow Pipelines is intentionally NOT here: upstream ships it as kustomize
# manifests (no official Helm chart), and kustomize-through-null_resource is
# brittle on destroy. scripts/deploy-kfp.sh installs it in ~2 minutes.
# -----------------------------------------------------------------------------

terraform {
  required_providers {
    aws        = { source = "hashicorp/aws" }
    kubernetes = { source = "hashicorp/kubernetes" }
    helm       = { source = "hashicorp/helm" }
    random     = { source = "hashicorp/random" }
  }
}

# --- EBS CSI driver -------------------------------------------------------------
resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = var.cluster_name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = var.ebs_csi_role_arn

  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

# --- Storage classes -------------------------------------------------------------
# EKS ships a legacy in-tree "gp2" class marked default; demote it so our gp3
# class (cheaper per GB, better baseline IOPS) picks up every PVC.
resource "kubernetes_annotations" "gp2_not_default" {
  api_version = "storage.k8s.io/v1"
  kind        = "StorageClass"
  force       = true # take ownership of the annotation from EKS

  metadata {
    name = "gp2"
  }

  annotations = {
    "storageclass.kubernetes.io/is-default-class" = "false"
  }
}

resource "kubernetes_storage_class_v1" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Delete" # demo: volumes vanish with their PVCs
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  parameters = {
    type      = "gp3"
    encrypted = "true"
  }

  depends_on = [aws_eks_addon.ebs_csi, kubernetes_annotations.gp2_not_default]
}

# --- metrics-server ----------------------------------------------------------------
resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  namespace  = "kube-system"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  version    = "3.12.1"
}

# --- AWS Load Balancer Controller ----------------------------------------------------
# Required by the brief; idles at ~50 MiB unless you actually create an
# Ingress/Service of type LoadBalancer (which this repo does not, by default).
resource "helm_release" "alb_controller" {
  name       = "aws-load-balancer-controller"
  namespace  = "kube-system"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "1.8.1"

  set {
    name  = "clusterName"
    value = var.cluster_name
  }

  set {
    name  = "region"
    value = var.region
  }

  set {
    name  = "vpcId"
    value = var.vpc_id
  }

  set {
    name  = "replicaCount" # chart default is 2 — pointless HA for a demo
    value = "1"
  }

  set {
    name  = "serviceAccount.create"
    value = "true"
  }

  set {
    name  = "serviceAccount.name"
    value = "aws-load-balancer-controller"
  }

  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = var.alb_controller_role_arn
  }

  set {
    # This webhook intercepts EVERY Service creation cluster-wide with
    # failurePolicy=Fail, so any Service created before the controller pod is
    # Ready errors out ("no endpoints available") — a race that breaks the
    # parallel install of the other addons. Its only purpose is to make this
    # controller the default for new LoadBalancer Services, and this demo
    # creates none, so switch it off.
    name  = "enableServiceMutatorWebhook"
    value = "false"
  }
}

# --- Cluster autoscaler -----------------------------------------------------------
resource "helm_release" "cluster_autoscaler" {
  name       = "cluster-autoscaler"
  namespace  = "kube-system"
  repository = "https://kubernetes.github.io/autoscaler"
  chart      = "cluster-autoscaler"
  # Chart RBAC must be new enough for the autoscaler image: 9.37.0's
  # ClusterRole lacks volumeattachments list/watch, which CA >= 1.33 requires —
  # the informers never sync and CA silently never scales anything.
  version = "9.58.0"

  set {
    name  = "autoDiscovery.clusterName"
    value = var.cluster_name
  }

  set {
    name  = "awsRegion"
    value = var.region
  }

  set {
    name  = "cloudProvider"
    value = "aws"
  }

  set {
    # Autoscaler image minor version must match the cluster's k8s minor.
    name  = "image.tag"
    value = "v${var.cluster_version}.0"
  }

  set {
    # Fixed SA name — must match the IRSA trust policy in modules/iam.
    name  = "rbac.serviceAccount.name"
    value = "cluster-autoscaler"
  }

  set {
    name  = "rbac.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = var.cluster_autoscaler_role_arn
  }

  # COST: scale empty nodes down fast. Defaults (10 min) would keep an idle
  # t3.xlarge alive for 10+ minutes after every pipeline run.
  set {
    name  = "extraArgs.scale-down-unneeded-time"
    value = "2m"
  }

  set {
    name  = "extraArgs.scale-down-delay-after-add"
    value = "2m"
  }

  set {
    name  = "extraArgs.expander"
    value = "least-waste"
  }

  set {
    name  = "extraArgs.balance-similar-node-groups"
    value = "true"
  }
}

# --- Apache Airflow ------------------------------------------------------------------
# Flask session secret; generated once and kept in state so pods don't get
# logged out on every apply.
resource "random_password" "airflow_webserver_secret" {
  length  = 32
  special = false
}

resource "helm_release" "airflow" {
  name             = "airflow"
  namespace        = "airflow"
  create_namespace = true
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = var.airflow_chart_version

  # DB migration + webserver boot on small spot nodes is slow; be patient.
  timeout = 900
  wait    = true

  values = [
    templatefile("${path.module}/values/airflow-values.yaml.tpl", {
      region               = var.region
      s3_bucket            = var.s3_bucket
      airflow_role_arn     = var.airflow_role_arn
      dags_repo_url        = var.dags_repo_url
      dags_repo_branch     = var.dags_repo_branch
      webserver_secret_key = random_password.airflow_webserver_secret.result
    })
  ]

  # Postgres PVC needs the CSI driver + default gp3 class; the autoscaler must
  # exist first in case Airflow needs the 2nd general node to schedule.
  depends_on = [
    kubernetes_storage_class_v1.gp3,
    helm_release.cluster_autoscaler,
  ]
}
