# -----------------------------------------------------------------------------
# Provider / Terraform version pins
# -----------------------------------------------------------------------------
terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # >= 5.62 needed for aws_eks_cluster.upgrade_policy (opting OUT of paid
      # extended support — see modules/eks/main.tf).
      version = "~> 5.70"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    helm = {
      # Deliberately pinned to 2.x: the 3.x provider changed the `kubernetes {}`
      # block to attribute syntax and would break providers.tf as written.
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # ---------------------------------------------------------------------------
  # STATE: local, on purpose.
  #
  # This is a throwaway single-operator demo. Local state means:
  #   + zero extra AWS resources (no state bucket / DynamoDB table to pay for
  #     or to leak after teardown)
  #   + no bootstrap chicken-and-egg (nothing must exist before `init`)
  #   - no locking, no team sharing, and if you delete this directory before
  #     running `terraform destroy` you orphan the whole stack.
  #
  # If you keep the cluster longer than a day or share it, switch to S3 state:
  #
  # backend "s3" {
  #   bucket       = "<your-tf-state-bucket>"   # create manually first
  #   key          = "eks-airflow-kubeflow-demo/terraform.tfstate"
  #   region       = "us-east-1"
  #   use_lockfile = true                       # S3-native locking, TF >= 1.10
  # }
  #
  # (With TF >= 1.10 `use_lockfile = true` replaces the old DynamoDB lock
  # table — one less resource to clean up.)
  # ---------------------------------------------------------------------------
}
