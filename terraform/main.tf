# -----------------------------------------------------------------------------
# Root module — wires vpc → eks → iam → addons together.
# -----------------------------------------------------------------------------

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  cluster_name = "${var.project_name}-eks"

  # EKS demands subnets in >= 2 AZs for the control plane; we use exactly 2
  # and nothing more (cost requirement: "single AZ where possible").
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

module "vpc" {
  source = "./modules/vpc"

  name         = var.project_name
  cluster_name = local.cluster_name
  vpc_cidr     = "10.0.0.0/16"
  azs          = local.azs
  enable_nat   = var.enable_nat
}

module "s3" {
  source = "./modules/s3"

  name_prefix = var.project_name
}

module "eks" {
  source = "./modules/eks"

  cluster_name      = local.cluster_name
  cluster_version   = var.cluster_version
  subnet_ids        = module.vpc.node_subnet_ids
  api_allowed_cidrs = var.api_allowed_cidrs

  general_instance_types   = var.general_instance_types
  pipelines_instance_types = var.pipelines_instance_types
  general_scaling          = var.general_scaling
  pipelines_scaling        = var.pipelines_scaling
  enable_pipelines_taint   = var.enable_pipelines_taint
}

module "iam" {
  source = "./modules/iam"

  cluster_name      = local.cluster_name
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer       = module.eks.oidc_issuer
  s3_bucket_arn     = module.s3.bucket_arn
}

module "addons" {
  source = "./modules/addons"

  cluster_name    = module.eks.cluster_name
  cluster_version = var.cluster_version
  region          = var.region
  vpc_id          = module.vpc.vpc_id

  ebs_csi_role_arn            = module.iam.ebs_csi_role_arn
  alb_controller_role_arn     = module.iam.alb_controller_role_arn
  cluster_autoscaler_role_arn = module.iam.cluster_autoscaler_role_arn
  airflow_role_arn            = module.iam.airflow_role_arn

  s3_bucket             = module.s3.bucket_name
  dags_repo_url         = var.dags_repo_url
  dags_repo_branch      = var.dags_repo_branch
  airflow_chart_version = var.airflow_chart_version

  # Helm releases need nodes to schedule onto — wait for the node groups.
  depends_on = [module.eks]
}
