variable "cluster_name" {
  type = string
}

variable "cluster_version" {
  type = string
}

variable "subnet_ids" {
  description = "Subnets for control-plane ENIs and nodes (2 AZs)."
  type        = list(string)
}

variable "api_allowed_cidrs" {
  type = list(string)
}

variable "general_instance_types" {
  type = list(string)
}

variable "pipelines_instance_types" {
  type = list(string)
}

variable "general_scaling" {
  type = object({
    min_size     = number
    max_size     = number
    desired_size = number
  })
}

variable "pipelines_scaling" {
  type = object({
    min_size     = number
    max_size     = number
    desired_size = number
  })
}

variable "enable_pipelines_taint" {
  type = bool
}
