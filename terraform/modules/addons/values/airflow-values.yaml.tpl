# -----------------------------------------------------------------------------
# Apache Airflow Helm chart overrides (rendered by Terraform templatefile()).
#
# Cost/simplicity decisions:
#   * KubernetesExecutor  → no Celery workers, no Redis; task pods are created
#     on demand and vanish afterwards
#   * bundled Postgres    → no RDS ($$), 8Gi gp3 PVC
#   * triggerer/statsd/pgbouncer/flower off → ~1.5 GiB RAM saved
#   * remote task logs in S3 via IRSA → survive pod deletion, no PVC for logs
# -----------------------------------------------------------------------------

executor: KubernetesExecutor

# Pin the app version explicitly so chart upgrades never surprise us.
airflowVersion: "2.10.5"
defaultAirflowTag: "2.10.5"

webserverSecretKey: ${webserver_secret_key}

# Injected into ALL Airflow containers, including dynamically-spawned task pods.
env:
  # Empty AWS connection → boto3 default credential chain → IRSA web identity.
  - name: AIRFLOW_CONN_AWS_DEFAULT
    value: "aws://"
  - name: AWS_DEFAULT_REGION
    value: "${region}"
  # Consumed by the sample DAGs.
  - name: DEMO_S3_BUCKET
    value: "${s3_bucket}"
  - name: DEMO_KFP_HOST
    value: "http://ml-pipeline.kubeflow.svc.cluster.local:8888"

config:
  core:
    load_examples: "False"
  logging:
    remote_logging: "True"
    remote_base_log_folder: "s3://${s3_bucket}/airflow-logs"
    remote_log_conn_id: "aws_default"
    encrypt_s3_logs: "False"

# DAGs come from git — no image baking, no PVC. Push to the repo, wait ≤60s.
dags:
  gitSync:
    enabled: true
    repo: "${dags_repo_url}"
    branch: "${dags_repo_branch}"
    subPath: "dags"   # only this folder is scanned for DAGs...
    depth: 1          # ...but the whole repo is cloned, so DAGs can read
    period: 60s       # ../pipelines/sklearn_pipeline.yaml too.

# Bundled Postgres instead of RDS — fine for a throwaway demo.
postgresql:
  enabled: true
  # Bitnami purged its old Docker Hub tags in 2025; the frozen mirrors live
  # under bitnamilegacy/. Without this the subchart image may fail to pull.
  image:
    repository: bitnamilegacy/postgresql
  primary:
    persistence:
      enabled: true
      size: 8Gi   # gp3 (cluster default StorageClass)

# Not needed with KubernetesExecutor.
redis:
  enabled: false

# RAM savers — none of these are needed for the demo.
triggerer:
  enabled: false
statsd:
  enabled: false
pgbouncer:
  enabled: false
flower:
  enabled: false

# Everything below runs on the 'general' t3.large nodes; requests are sized
# so scheduler + webserver + postgres fit a single node at bootstrap.
webserver:
  replicas: 1
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: ${airflow_role_arn}
  resources:
    requests:
      cpu: 250m
      memory: 1Gi
    limits:
      memory: 1536Mi

scheduler:
  replicas: 1
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: ${airflow_role_arn}
  resources:
    requests:
      cpu: 250m
      memory: 768Mi
    limits:
      memory: 1Gi

# With KubernetesExecutor the "workers" section defines the TASK POD template:
# this SA annotation is what gives every task pod S3 access via IRSA.
workers:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: ${airflow_role_arn}
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      memory: 1536Mi

# Task logs live in S3; no shared log volume needed.
logs:
  persistence:
    enabled: false
