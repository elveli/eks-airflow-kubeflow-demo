# -----------------------------------------------------------------------------
# Input variables — everything cost-relevant is a variable so you can dial
# the demo up/down without editing modules.
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Short prefix for all resource names. Keep it short (used in IAM role names, bucket names)."
  type        = string
  default     = "afkf-demo"
}

variable "region" {
  description = "AWS region. us-west-2 (Oregon): cheapest-tier US pricing AND low latency from the US West Coast. The spot-price spread between regions is pennies/day — pick whatever is closest to you."
  type        = string
  default     = "us-west-2"
}

variable "cluster_version" {
  description = <<-EOT
    EKS Kubernetes version. COST TRAP: clusters on a version past its ~14-month
    standard-support window bill at $0.60/h (extended support) instead of
    $0.10/h. Always set this to one of the newest available versions.
  EOT
  type        = string
  default     = "1.33"
}

variable "api_allowed_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Tighten to <your-ip>/32 if you like."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enable_nat" {
  description = <<-EOT
    false (default) = nodes live in PUBLIC subnets with public IPs and no NAT
    gateway at all — the cheapest option ($0 instead of ~$1.10+/day for NAT).
    Nodes stay protected by the cluster security group (no inbound open).
    true = classic private subnets + a SINGLE NAT gateway (not per-AZ).
  EOT
  type        = bool
  default     = false
}

variable "dags_repo_url" {
  description = <<-EOT
    HTTPS URL of the git repo Airflow's git-sync pulls DAGs from — normally
    your fork/copy of THIS repo, pushed to GitHub as a PUBLIC repo (private
    repos need git-sync credentials; see README). No default on purpose:
    you must set it in terraform.tfvars.
  EOT
  type        = string
}

variable "dags_repo_branch" {
  description = "Branch git-sync tracks."
  type        = string
  default     = "main"
}

# --- Node groups: SPOT everywhere, tiny counts -------------------------------

variable "general_instance_types" {
  description = "Instance types for the 'general' node group (Airflow + KFP control plane + system). Multiple types = better spot availability."
  type        = list(string)
  default     = ["t3.large", "m5.large"] # 2 vCPU / 8 GiB each
}

variable "pipelines_instance_types" {
  description = "Instance types for the 'pipelines' node group (KFP executor pods). Scales from zero."
  type        = list(string)
  default     = ["t3.xlarge", "m5.xlarge"] # 4 vCPU / 16 GiB each
}

variable "general_scaling" {
  description = "Scaling for the general node group. Steady state is usually 2 nodes once KFP is installed."
  type = object({
    min_size     = number
    max_size     = number
    desired_size = number
  })
  default = {
    min_size     = 1
    max_size     = 2
    desired_size = 1
  }
}

variable "pipelines_scaling" {
  description = "Scaling for the pipelines node group. min/desired 0 = costs nothing while idle; the cluster autoscaler scales it up when a KFP run is submitted."
  type = object({
    min_size     = number
    max_size     = number
    desired_size = number
  })
  default = {
    min_size     = 0
    max_size     = 2
    desired_size = 0
  }
}

variable "enable_pipelines_taint" {
  description = <<-EOT
    Taint the pipelines node group (workload=pipelines:NoSchedule) so ONLY
    KFP executor pods (which add a matching toleration via kfp-kubernetes)
    land there — guarantees clean scale-back-to-zero. Set false if you hit
    toleration issues and just rely on the node selector.
  EOT
  type        = bool
  default     = true
}

variable "airflow_chart_version" {
  description = "Official Apache Airflow Helm chart version (deploys Airflow 2.10.x by default)."
  type        = string
  default     = "1.16.0"
}
