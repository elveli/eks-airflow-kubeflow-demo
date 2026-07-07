variable "cluster_name" {
  type = string
}

variable "oidc_provider_arn" {
  description = "ARN of the cluster's IAM OIDC provider."
  type        = string
}

variable "oidc_issuer" {
  description = "OIDC issuer URL without the https:// scheme."
  type        = string
}

variable "s3_bucket_arn" {
  type = string
}
