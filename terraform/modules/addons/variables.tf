variable "cluster_name" {
  type = string
}

variable "cluster_version" {
  description = "Used to pin the cluster-autoscaler image to the matching k8s minor."
  type        = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "ebs_csi_role_arn" {
  type = string
}

variable "alb_controller_role_arn" {
  type = string
}

variable "cluster_autoscaler_role_arn" {
  type = string
}

variable "airflow_role_arn" {
  type = string
}

variable "s3_bucket" {
  type = string
}

variable "dags_repo_url" {
  type = string
}

variable "dags_repo_branch" {
  type = string
}

variable "airflow_chart_version" {
  type = string
}
