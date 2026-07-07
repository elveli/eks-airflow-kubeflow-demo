output "region" {
  description = "AWS region (consumed by the helper scripts)."
  value       = var.region
}

output "cluster_name" {
  description = "EKS cluster name (consumed by the helper scripts)."
  value       = module.eks.cluster_name
}

output "configure_kubectl" {
  description = "Run this once after apply."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "s3_bucket" {
  description = "Bucket holding Airflow logs (airflow-logs/) and KFP model artifacts (kfp-artifacts/)."
  value       = module.s3.bucket_name
}

output "airflow_irsa_role_arn" {
  description = "IAM role assumed by Airflow pods via IRSA."
  value       = module.iam.airflow_role_arn
}

output "kfp_irsa_role_arn" {
  description = "IAM role for the KFP pipeline-runner service account (scripts/deploy-kfp.sh annotates it)."
  value       = module.iam.kfp_role_arn
}

output "airflow_ui" {
  description = "Port-forward command for the Airflow UI (login admin/admin)."
  value       = "kubectl -n airflow port-forward svc/airflow-webserver 8080:8080  →  http://localhost:8080"
}

output "kubeflow_ui" {
  description = "Port-forward command for the Kubeflow Pipelines UI."
  value       = "kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8081:80  →  http://localhost:8081"
}

output "next_step" {
  description = "Kubeflow Pipelines is NOT installed by Terraform (see README)."
  value       = "Run: ./scripts/deploy-kfp.sh"
}
