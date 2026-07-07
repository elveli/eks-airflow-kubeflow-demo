# -----------------------------------------------------------------------------
# One bucket for everything (cheaper/simpler than two):
#   airflow-logs/    — Airflow remote task logs (expired after 7 days)
#   kfp-artifacts/   — model artifacts published by the KFP pipeline
#   etl/             — output of the sample ETL DAG
# -----------------------------------------------------------------------------

terraform {
  required_providers {
    aws    = { source = "hashicorp/aws" }
    random = { source = "hashicorp/random" }
  }
}

# Bucket names are globally unique — add a random suffix.
resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "this" {
  bucket = "${var.name_prefix}-mlops-${random_id.suffix.hex}"

  # THROWAWAY DEMO: force_destroy lets `terraform destroy` delete the bucket
  # even when it contains objects. Without this, destroy fails on a non-empty
  # bucket and the bucket (plus storage billing) leaks.
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3: free, no KMS charges
    }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  # Task logs are worthless after a few days in a demo.
  rule {
    id     = "expire-airflow-logs"
    status = "Enabled"

    filter {
      prefix = "airflow-logs/"
    }

    expiration {
      days = 7
    }
  }

  # Never pay for half-uploaded junk.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
