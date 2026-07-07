output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.this.endpoint
}

output "cluster_ca_data" {
  value = aws_eks_cluster.this.certificate_authority[0].data
}

output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.this.arn
}

# Issuer URL without the https:// scheme — the form IAM trust policies expect.
output "oidc_issuer" {
  value = trimprefix(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://")
}
