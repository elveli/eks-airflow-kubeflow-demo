variable "name" {
  description = "Name prefix."
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name — used only for the kubernetes.io subnet tags."
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Exactly the AZs to use (2 for this demo)."
  type        = list(string)
}

variable "enable_nat" {
  description = "true = private node subnets behind a single NAT gateway; false = nodes in public subnets, no NAT (cheapest)."
  type        = bool
  default     = false
}
