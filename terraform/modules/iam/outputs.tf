output "airflow_role_arn" {
  value = aws_iam_role.irsa["airflow"].arn
}

output "kfp_role_arn" {
  value = aws_iam_role.irsa["kfp"].arn
}

output "cluster_autoscaler_role_arn" {
  value = aws_iam_role.irsa["cluster-autoscaler"].arn
}

output "alb_controller_role_arn" {
  value = aws_iam_role.irsa["alb-controller"].arn
}

output "ebs_csi_role_arn" {
  value = aws_iam_role.irsa["ebs-csi"].arn
}
