# -----------------------------------------------------------------------------
# Providers
# -----------------------------------------------------------------------------
provider "aws" {
  region = var.region

  # Every resource gets these tags — this is also what the orphan-cleanup
  # script keys off, so keep Project stable.
  default_tags {
    tags = {
      Project     = "eks-airflow-kubeflow-demo"
      Environment = "demo"
      ManagedBy   = "terraform"
    }
  }
}

# -----------------------------------------------------------------------------
# kubernetes/helm providers are configured from the EKS module's outputs.
# Configuring a provider from resources created in the same config is a
# documented anti-pattern (the provider config is unknown until the cluster
# exists), but it is acceptable for a demo and works on a clean apply because
# every kubernetes/helm resource depends on module.eks.
#
# If a plan ever fails with "cluster unreachable", do a two-phase apply:
#   terraform apply -target=module.vpc -target=module.eks -target=module.iam
#   terraform apply
# -----------------------------------------------------------------------------
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_ca_data)

  # exec auth: tokens are minted on demand by the AWS CLI, so they never
  # expire mid-apply (unlike a static data.aws_eks_cluster_auth token).
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_ca_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}
