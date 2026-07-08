# Airflow + Kubeflow Pipelines on EKS — cost-optimized demo

A **throwaway showcase** that provisions an EKS cluster with Terraform and runs
**Apache Airflow** (KubernetesExecutor) orchestrating a **Kubeflow Pipelines
(KFP)** scikit-learn training pipeline end-to-end, with artifacts and logs in
S3 via IRSA.

Naming notes, so nothing reads like line noise:

* **KFP** = **K**ubeflow **P**ipelines — the ML-workflow engine from the
  Kubeflow project. We deploy only this component, not the full Kubeflow
  platform (see design decisions below).
* **afkf** = **a**ir**f**low + **k**ube**f**low — nothing more than the default
  `project_name` prefix stamped on every AWS resource this repo creates
  (`afkf-demo-eks` cluster, `afkf-demo-mlops-*` bucket, IAM roles…). Change it
  via `project_name` in `terraform.tfvars` if you prefer, but only before the
  first `apply`.

Everything is tuned for **minimum burn rate**, not production resilience:
SPOT nodes, no NAT gateway, no RDS, no load balancers, a scale-from-zero node
group for ML workloads, and a kill switch that parks the whole cluster at
~$2.50/day.

## Contents

- [Architecture](#architecture)
- [What the files in `dags/` are, and why they exist](#what-the-files-in-dags-are-and-why-they-exist)
- [Airflow vs. Kubeflow Pipelines — why both?](#airflow-vs-kubeflow-pipelines--why-both)
- [💸 Cost estimate](#-cost-estimate)
- [Prerequisites](#prerequisites)
- [Deploy](#deploy)
  - [Run the demo](#run-the-demo)
  - [Container images: what gets pulled, and from where](#container-images-what-gets-pulled-and-from-where)
  - [Manual steps, called out honestly](#manual-steps-called-out-honestly)
  - [Inspecting the Helm releases from the CLI](#inspecting-the-helm-releases-from-the-cli)
  - [What a healthy system looks like](#what-a-healthy-system-looks-like)
  - [Sidecars: why READY says 3/3](#sidecars-why-ready-says-33)
- [🔴 Cost kill switch (park it, don't destroy it)](#-cost-kill-switch-park-it-dont-destroy-it)
- [Teardown](#teardown)
  - [Resources that can leak and keep billing](#resources-that-can-leak-and-keep-billing)
- [Design decisions & tradeoffs](#design-decisions--tradeoffs)
- [Optional: expose Airflow via ALB](#optional-expose-airflow-via-alb)
- [Known gotchas](#known-gotchas)
- [Repository layout](#repository-layout)

---

## Architecture

```mermaid
flowchart TB
    subgraph laptop["💻 Your laptop"]
        TF["terraform apply"]
        PF["kubectl port-forward\nAirflow :8080 / KFP :8081"]
    end

    subgraph AWS["AWS account · 1 region · 2 AZs"]
        S3[("S3 bucket\nairflow-logs/ · kfp-artifacts/ · etl/")]

        subgraph VPC["VPC 10.0.0.0/16 — public subnets, NO NAT gateway"]
            CP["EKS control plane v1.33\n$0.10/h"]

            subgraph NG1["node group: general\nt3.large SPOT × 1-2"]
                AF["Airflow\nscheduler · webserver · Postgres (PVC)"]
                KFPC["KFP control plane\nml-pipeline · UI · MySQL · seaweedfs (PVCs)"]
                SYS["cluster-autoscaler · ALB controller\nEBS CSI · metrics-server"]
            end

            subgraph NG2["node group: pipelines\nt3.xlarge SPOT × 0-2 (scale from ZERO)"]
                EXEC["KFP executor pods\ntrain → evaluate+publish"]
            end
        end
    end

    GH["GitHub repo\n(dags/ + compiled pipeline)"] -- "git-sync (60s)" --> AF
    TF --> AWS
    PF --> AF & KFPC
    AF -- "task pod: kfp SDK submit + wait" --> KFPC
    KFPC -- "spawns pods (autoscaler: 0→1 node)" --> EXEC
    AF -- "IRSA: task logs" --> S3
    EXEC -- "IRSA: model.joblib + metrics.json" --> S3
```

**The demo flow:** trigger the `train_on_kubeflow` DAG in Airflow → an Airflow
task pod submits `pipelines/sklearn_pipeline.yaml` to the KFP API and waits →
KFP schedules executor pods pinned to the `pipelines` node group → the cluster
autoscaler boots a t3.xlarge **from zero** → the pipeline trains/evaluates a
RandomForest and uploads the model to S3 → the node scales back to zero ~2 min
later → a final Airflow task lists the artifacts in S3.

## What the files in `dags/` are, and why they exist

In Airflow, every workflow is a **DAG** (directed acyclic graph) **defined as
a Python file**. Airflow scans a folder, imports each `.py` file it finds, and
every DAG defined inside becomes a runnable, schedulable workflow in the UI —
no separate deploy step. Without these files Airflow would boot fine but sit
completely empty: they *are* the demo content.

**How they get into the cluster:** you never copy them anywhere. A git-sync
sidecar inside the Airflow pods clones **this GitHub repo** (the
`dags_repo_url` variable) and re-pulls every 60 seconds; Airflow scans the
repo's `dags/` folder. Push a change to `main` and it shows up in the UI
within a minute — that's also why this repo must be reachable (public) from
the cluster.

#### git-sync, demystified — do I ever need to push?

**No push is ever required to *run* anything.** git-sync answers one question
only: *"what code does Airflow have?"* — it has nothing to do with *when*
that code runs. Keep the two ideas separate:

| Act | What it affects | How |
|---|---|---|
| **git push** to this repo | Which DAG **code** Airflow has (≤60 s later) | Edit `dags/*.py`, commit, push — that's the whole "deploy" |
| **Trigger** a DAG | Whether a **run** happens | ▶ in the UI, `airflow dags trigger`, or a `schedule=` in the DAG file |

The mechanics: git-sync is a tiny sidecar container in the scheduler pod that
just runs `git pull` in a loop, every 60 seconds, forever. It's a **pull
model** — no webhooks, nothing on GitHub's side, no reaction *to* your push;
the cluster simply notices the new commit at the next poll. Pushing never
starts a DAG run, and triggering a DAG never touches git.

So for this demo: the repo already contains both DAGs and the compiled
pipeline, so **zero pushes are needed** — deploy, open the UI, trigger. You
only push when you *change* something (edit a DAG, recompile the pipeline
YAML), and the push replaces rebuilding an image or re-running Helm as the
deployment mechanism. That's the entire reason the chart offers git-sync:
DAG code changes constantly, and nobody wants an image build per edit.

One nuance for completeness: a *newly added* DAG appears in the UI paused
(that's the `paused` event you see in the audit log) — it can't run until
unpaused, which is a safety default, not git-sync behavior.

The two files:

| File | DAG in the UI | What it does | Why it's here |
|---|---|---|---|
| `dags/etl_simple.py` | `etl_simple` | Generates 1 000 fake order rows → aggregates revenue per city → writes `summary.json` to `s3://<bucket>/etl/<date>/` | Smoke test. Proves Airflow schedules task pods and that they can write to S3 **with no credentials configured** (IRSA). Run it first; it finishes in ~1 min. |
| `dags/trigger_kubeflow_pipeline.py` | `train_on_kubeflow` | Submits the compiled KFP package (`pipelines/sklearn_pipeline.yaml`) to the in-cluster KFP API, waits for the run to succeed, then lists the model artifacts that landed in `s3://<bucket>/kfp-artifacts/` | The headline: Airflow *orchestrating* Kubeflow. The KFP SDK isn't baked into the Airflow image — the task pip-installs it in a throwaway virtualenv at runtime. |

Related but different: the `pipelines/` folder is **not** Airflow code — it's
the Kubeflow pipeline (`sklearn_pipeline.py` source → compiled
`sklearn_pipeline.yaml`) that the second DAG submits. It rides along in the
same git-sync clone, which is how the DAG finds the YAML at
`../pipelines/sklearn_pipeline.yaml`.

## Airflow vs. Kubeflow Pipelines — why both?

Both run DAGs of tasks, so they look similar at first. The difference is what
they're for:

**Airflow is the general-purpose scheduler and orchestrator.** It answers
"*when* should things run, in what order, and what happens on failure?" —
cron schedules, retries, backfills, alerting, and hundreds of integrations
(S3, databases, Spark, dbt…). A task can be anything; Airflow doesn't care
that one of them happens to be "ML".

**Kubeflow Pipelines is an ML-specific execution engine on Kubernetes.** It
answers "*how* do I run an ML workflow reproducibly?" — every step is a
container with typed inputs/outputs, and KFP tracks the artifacts (datasets,
models), metrics (the accuracy on the evaluate step) and lineage across runs,
supports experiments for comparing runs, and lets each step demand its own
resources. That last part is how this demo's pipeline pins itself to the
scale-from-zero node group — and how a real one would request GPUs for
training only.

| | Airflow | Kubeflow Pipelines |
|---|---|---|
| Center of gravity | Scheduling + integration ("data platform conductor") | ML reproducibility + artifact/metric tracking |
| A "task" is | Any Python/operator code | A container with typed inputs/outputs |
| Killer features | Cron, retries, backfills, 100s of integrations | Artifact lineage, experiments, per-step resources (GPUs) |
| Who lives in it | Data engineers | ML engineers / data scientists |

**Why both, together:** in real orgs Airflow is the system of record for "the
nightly retraining runs at 2am, *after* the ETL that produces the training
data succeeded, and pages someone if it fails" — while the ML work itself
runs in KFP, where models, metrics and lineage are tracked. That handoff is
exactly what this repo demonstrates: `etl_simple` is a pure-Airflow job;
`train_on_kubeflow` is Airflow delegating to KFP with one API call and
waiting for the verdict.

If you only need one: a pure data-engineering shop is fine with Airflow alone
(it can run training scripts too — it just won't track models or experiments
for you), and an ML-only team can live in KFP alone (it has recurring runs) —
until their pipelines start depending on upstream business data, which is
where Airflow's integration breadth earns its keep.

---

## 💸 Cost estimate

On-demand baselines: t3.large ≈ $0.083/h, t3.xlarge ≈ $0.166/h; SPOT is
typically 60–70 % off. Numbers below assume us-west-2 and will drift — treat
as ±30 %.

| Component | Qty (steady state) | ~$/hour | ~$/day |
|---|---|---|---|
| EKS control plane (standard support) | 1 | 0.100 | 2.40 |
| t3.large SPOT (`general`) | 2 | 0.055 | 1.32 |
| t3.xlarge SPOT (`pipelines`) | **0** idle / 1 during runs | 0 – 0.055 | ~0 |
| EBS gp3 (3× 20 GiB node roots + ~48 GiB PVCs) | ~110 GiB | 0.012 | 0.29 |
| S3 + requests | few GiB | ~0.001 | 0.03 |
| NAT gateway | **0 (disabled by default)** | (0.045 + $0.045/GiB if enabled) | — |
| **Total — idle** | | **≈ 0.17** | **≈ 4.05** |
| **Total — while a pipeline runs** | | ≈ 0.22 | — |
| **Kill switch ON (nodes = 0)** | | **≈ 0.105** | **≈ 2.55** |

Cost traps this repo already avoids — don't reintroduce them:

* **EKS extended support = $0.60/h** (6× the control plane price). Clusters on
  an outdated Kubernetes version get billed this automatically. `cluster_version`
  defaults to a recent version **and** `upgrade_policy.support_type = "STANDARD"`
  opts out of paid extended support entirely.
* **NAT gateway** — off by default; nodes sit in public subnets (inbound still
  closed by the cluster security group).
* **Control-plane CloudWatch logs** — not enabled.
* **Load balancers** — nothing in this repo creates one; UIs are port-forwarded.

---

## Prerequisites

* Terraform ≥ 1.7, AWS CLI v2 (authenticated, e.g. `aws sts get-caller-identity` works), `kubectl`, `git`, `jq`
* Helm is **not** needed locally (Terraform's helm provider does the installs)
* A **public GitHub repo** containing this code — Airflow's git-sync pulls DAGs
  from it (private repos work too but need git-sync credentials; simplest to
  keep the demo repo public)
* Python ≤ 3.12 only if you want to *recompile* the pipeline
  (`pipelines/sklearn_pipeline.yaml` is already compiled and committed)

## Deploy

```bash
# 0. Push this repo to GitHub, then point Airflow's git-sync at it:
cd terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars          # set dags_repo_url (required)

# 1. Provision everything: VPC, EKS, IRSA, S3, addons, Airflow (~20 min)
terraform init
terraform apply

# 2. Point kubectl at the cluster
aws eks update-kubeconfig --region "$(terraform output -raw region)" --name "$(terraform output -raw cluster_name)"

# 3. Install Kubeflow Pipelines standalone (~5 min; see "manual steps" below for why)
cd .. && ./scripts/deploy-kfp.sh

# 4. Open the UIs (no load balancers — SSH-tunnel-style local access)
./scripts/port-forward.sh
#    Airflow → http://localhost:8080  (admin / admin)
#    KFP     → http://localhost:8081
```

Or with make: `make deploy` then `make pf`.

### Run the demo

**Step 1 — smoke test with the ETL DAG (~1 min).**
Open the Airflow UI at http://localhost:8080 (admin / admin). On the DAGs
page, find `etl_simple`, click its pause toggle to unpause it, then press the
▶ (Trigger) button. Within a minute all three tasks should turn dark green
(success). This proves the basics work: Airflow can launch task pods, and
those pods can write to S3 without any credentials (IRSA). Verify the output
landed:

```bash
aws s3 ls --recursive "s3://$(terraform -chdir=terraform output -raw s3_bucket)/etl/"
```

**Step 2 — the main event: trigger `train_on_kubeflow`.**
Unpause and trigger it the same way. Expect the **first run to take ~10
minutes**, because three one-time things happen back to back:

1. the Airflow task pod pip-installs the KFP SDK into a throwaway virtualenv (~2 min),
2. the cluster autoscaler boots a spot t3.xlarge **from zero** for the pipeline pods (~3 min),
3. the pipeline container images are pulled for the first time (~2 min).

Repeat runs skip the node wait and image pulls and finish in ~3–4 minutes.

**Step 3 — watch it happen (optional but the whole point of a demo).**
In a couple of terminals while the run is going:

```bash
kubectl get nodes -w                 # an extra node appears (workload=pipelines), then vanishes ~2 min after the run
kubectl -n kubeflow get pods -w      # the train / evaluate executor pods come and go
```

And in the KFP UI at http://localhost:8081 → *Runs* → the newest
`airflow-triggered-*` run shows the live pipeline graph with per-step logs.

**Step 4 — confirm the result.**
The Airflow run goes green once the pipeline succeeds and the final task has
verified the artifacts; the trained model is now in S3:

```bash
aws s3 ls --recursive "s3://$(terraform -chdir=terraform output -raw s3_bucket)/kfp-artifacts/"
# → .../<timestamp>/model.joblib  and  .../<timestamp>/metrics.json
```

After one run of each DAG, the whole bucket looks like this
(`aws s3 ls --recursive s3://<bucket>/` to see it flat):

```
s3://afkf-demo-mlops-<hex>/
├── airflow-logs/                        # every task's log, written via IRSA (7-day expiry)
│   ├── dag_id=etl_simple/
│   │   └── run_id=manual__2026-07-08T18:02:11+00:00/
│   │       ├── task_id=extract/attempt=1.log
│   │       ├── task_id=transform/attempt=1.log
│   │       └── task_id=load/attempt=1.log
│   └── dag_id=train_on_kubeflow/
│       └── run_id=manual__2026-07-08T18:10:42+00:00/
│           ├── task_id=submit_kfp_run/attempt=1.log
│           └── task_id=report_artifacts/attempt=1.log
├── etl/
│   └── 2026-07-08/
│       └── summary.json                 # DAG 1 output: revenue-per-city aggregate
└── kfp-artifacts/
    └── 20260708-181530/                 # UTC timestamp of the KFP run
        ├── model.joblib                 # the trained RandomForest
        └── metrics.json                 # {"accuracy": 0.97..., "published_at": ...}
```

The `dag_id=/run_id=/task_id=` layout is Airflow's default remote-log naming —
it means the S3 console doubles as a browsable log archive. Note that KFP's
*intermediate* artifacts (what you see attached to steps in the KFP UI) live
in the in-cluster seaweedfs object store, not here: only what the `evaluate_and_publish`
component explicitly uploads reaches this bucket.

> **MinIO / seaweedfs, in one breath:** KFP can't assume it runs on AWS, so it
> bundles an in-cluster object store that *speaks the S3 API* for its internal
> artifacts. For years that was **MinIO** ("self-hosted S3"); KFP 2.14+ swapped
> it for **seaweedfs** (MinIO's move to AGPL licensing being a big driver) —
> though relics like the `mlpipeline-minio-artifact` secret name remain. The
> pattern to copy either way: the engine's scratch space is disposable and
> cluster-local, while anything that matters gets published to real S3.

And the real thing — actual listings from a working deployment after three
pipeline runs and one ETL run:

```
$ aws s3 ls --recursive "s3://$(terraform -chdir=terraform output -raw s3_bucket)/kfp-artifacts/"
2026-07-08 16:03:22         52 kfp-artifacts/20260708-230321/metrics.json
2026-07-08 16:03:22     275265 kfp-artifacts/20260708-230321/model.joblib
2026-07-08 16:16:00         52 kfp-artifacts/20260708-231559/metrics.json
2026-07-08 16:16:00     275265 kfp-artifacts/20260708-231559/model.joblib
2026-07-08 16:24:03         52 kfp-artifacts/20260708-232402/metrics.json
2026-07-08 16:24:03     275265 kfp-artifacts/20260708-232402/model.joblib

$ aws s3 ls --recursive "s3://$(terraform -chdir=terraform output -raw s3_bucket)/airflow-logs/"
2026-07-08 15:54:12      63370 airflow-logs/dag_id=etl_simple/run_id=manual__2026-07-08T22:53:27.214841+00:00/task_id=extract/attempt=1.log
2026-07-08 15:54:48       2826 airflow-logs/dag_id=etl_simple/run_id=manual__2026-07-08T22:53:27.214841+00:00/task_id=load/attempt=1.log
2026-07-08 15:54:32       3118 airflow-logs/dag_id=etl_simple/run_id=manual__2026-07-08T22:53:27.214841+00:00/task_id=transform/attempt=1.log
2026-07-08 16:03:55       9345 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T22:59:50.981548+00:00/task_id=report_artifacts/attempt=1.log
2026-07-08 16:03:37      26243 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T22:59:50.981548+00:00/task_id=submit_kfp_run/attempt=1.log
2026-07-08 16:16:51       3577 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T23:12:45+00:00/task_id=report_artifacts/attempt=1.log
2026-07-08 16:16:32      26187 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T23:12:45+00:00/task_id=submit_kfp_run/attempt=1.log
2026-07-08 16:24:40       3961 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T23:20:53.838844+00:00/task_id=report_artifacts/attempt=1.log
2026-07-08 16:24:21      26245 airflow-logs/dag_id=train_on_kubeflow/run_id=manual__2026-07-08T23:20:53.838844+00:00/task_id=submit_kfp_run/attempt=1.log
```

Three details worth noticing in there:

* Every `model.joblib` is byte-identical in size (275 265) — training is
  fully deterministic (`random_state=42`, bundled dataset), so each run grows
  the same forest. Reproducibility, visible from a directory listing.
* The first `train_on_kubeflow` run's logs include a `report_artifacts`
  attempt — that run's reporting task *failed* (a since-fixed bug), and its
  failure log was shipped to S3 like any other. Remote logging captures
  failures, which is the whole point.
* `submit_kfp_run` logs are ~26 KB vs ~3 KB for other tasks: that's the
  buffered virtualenv build (pip install of the KFP SDK) dumped into the log.

### Container images: what gets pulled, and from where

The "pulling images" wait in step 2 happens on the freshly scaled-from-zero
node, which always starts with an empty image cache:

| Image | Registry | Role |
|---|---|---|
| `python:3.11-slim` | Docker Hub | Base image for both pipeline components (set in `pipelines/sklearn_pipeline.py`) |
| `ghcr.io/kubeflow/kfp-launcher` | GitHub (ghcr.io) | Injected by KFP v2 into every executor pod to shuttle inputs/outputs |
| `quay.io/argoproj/argoexec` | Red Hat (quay.io) | Argo sidecar supervising each step's container |
| `amazon-k8s-cni`, `kube-proxy`, `ebs-csi-node` | Amazon regional ECR | DaemonSets every brand-new node pulls before it's Ready |

The components' Python deps (`scikit-learn`, `boto3`) are in **no** image —
KFP pip-installs them at container start (`packages_to_install`), which is a
separate delay paid on *every* run, image cache or not.

The rest of the stack is pulled once at deploy time:

* **Airflow** (`airflow` ns): `apache/airflow:2.10.5` (Docker Hub),
  `registry.k8s.io/git-sync/git-sync`, `bitnamilegacy/postgresql` (Docker Hub)
* **KFP control plane** (`kubeflow` ns): ~10 images, nearly all
  `ghcr.io/kubeflow/kfp-*` (api-server, frontend, persistence-agent…), plus
  `mysql:8.4` and `chrislusf/seaweedfs` from Docker Hub,
  `quay.io/argoproj/workflow-controller`, and one gcr.io holdout
  (`gcr.io/tfx-oss-public/ml_metadata_store_server`)
* **Addons** (`kube-system`): `registry.k8s.io/autoscaling/cluster-autoscaler`,
  `registry.k8s.io/metrics-server/*`, `public.ecr.aws/eks/aws-load-balancer-controller`

So six public registries in total — Docker Hub, ghcr.io, quay.io,
registry.k8s.io, gcr.io, ECR (public + regional) — all reached over the
nodes' public-subnet egress,
which is why the no-NAT design still needs internet access. Docker Hub pulls
are anonymous and **rate-limited**: this demo normally stays well under the
limit, but if you ever see `ErrImagePull` / `toomanyrequests` on
`python:3.11-slim` or the Airflow image, that's it — wait a few minutes, or
switch the base image to `public.ecr.aws/docker/library/python:3.11-slim`
(same image, mirrored on ECR Public, no Docker Hub limits).

**Where the pulls land:** on each node's own 20 GiB gp3 root EBS volume
(`disk_size` in `modules/eks`). EKS AL2023 nodes run containerd, which keeps
image layers under `/var/lib/containerd/`. Three consequences worth knowing:

* The cache is **per node and dies with the node**. That's exactly why the
  scale-from-zero `pipelines` node re-pulls everything on every scale-up (and
  why the `general` nodes, which live long, only pay the pull once). Nodes
  don't share layers — two nodes pulling the same image download it twice.
* The kubelet garbage-collects old images if the disk passes ~85 % full;
  this stack peaks at ~4–5 GiB of images, so 20 GiB never gets close.
* You can inspect a node's cache without SSH — Kubernetes reports it in node
  status:

  ```bash
  kubectl get node <node-name> \
    -o jsonpath='{range .status.images[*]}{.sizeBytes}{"\t"}{.names[-1]}{"\n"}{end}' | sort -rn | head
  ```

If the repeat pulls on scale-from-zero ever bothered you (for this demo they
shouldn't), the standard fixes are an ECR pull-through cache, a warm pool, or
just accepting the ~2 minutes — this repo does the latter.

If `terraform apply` ever fails with a Kubernetes-provider connection error
(rare bootstrap race), run it in two phases:
`terraform apply -target=module.vpc -target=module.eks -target=module.iam && terraform apply`.

### Manual steps, called out honestly

1. **Push the repo + set `dags_repo_url`** — git-sync needs a reachable repo;
   there's no way around this without baking DAGs into an image.
2. **`./scripts/deploy-kfp.sh` after apply** — upstream ships KFP standalone as
   kustomize manifests only (no official Helm chart). Wrapping `kubectl apply -k`
   in a Terraform `null_resource` makes destroys flaky, so it's an explicit,
   idempotent script instead. It also annotates KFP's `pipeline-runner` service
   account with the IRSA role ARN (Terraform can't — the SA doesn't exist until
   the manifests are applied).
3. **Unpause the DAGs** in the Airflow UI before triggering.

### Inspecting the Helm releases from the CLI

Terraform installs everything through Helm, so the `helm` CLI is the fastest
way to see what's actually configured. It talks to your current kubeconfig
context — run `make kubeconfig` first (and `kubectl config current-context`
if you're not sure which cluster you're pointed at).

```bash
helm list -A                                    # all releases in all namespaces
helm get values airflow -n airflow              # the overrides Terraform passed in
helm get values airflow -n airflow --all        # merged with chart defaults = effective config
helm get manifest airflow -n airflow            # the rendered Kubernetes YAML
helm status airflow -n airflow                  # release health / last deploy result
helm history cluster-autoscaler -n kube-system  # revision history of a release
```

To browse a chart's *available* settings before changing the values template:

```bash
helm show values airflow --repo https://airflow.apache.org --version 1.16.0 | less
helm show values aws-load-balancer-controller --repo https://aws.github.io/eks-charts --version 1.8.1
```

### What a healthy system looks like

Real `kubectl get pods -A` output from a working deployment (2 general nodes,
a few pipeline runs done), for comparing against your own install:

```
NAMESPACE     NAME                                                         READY   STATUS      RESTARTS      AGE
airflow       airflow-postgresql-0                                         1/1     Running     0             79m
airflow       airflow-scheduler-68754498f7-5jp67                           3/3     Running     0             80m
airflow       airflow-webserver-59697545dc-wc2sk                           1/1     Running     1 (77m ago)   80m
kube-system   aws-load-balancer-controller-c76568c54-98hkv                 1/1     Running     0             80m
kube-system   aws-node-fb4f2                                               2/2     Running     0             79m
kube-system   aws-node-ktlxs                                               2/2     Running     0             90m
kube-system   cluster-autoscaler-aws-cluster-autoscaler-7b875f97f-9n9gr    1/1     Running     0             84m
kube-system   coredns-7c8cff8c-q4txj                                       1/1     Running     0             80m
kube-system   coredns-7c8cff8c-qsthq                                       1/1     Running     0             79m
kube-system   ebs-csi-controller-6c4b985d89-697tx                          6/6     Running     0             80m
kube-system   ebs-csi-controller-6c4b985d89-6tm45                          6/6     Running     0             79m
kube-system   ebs-csi-node-75w97                                           3/3     Running     0             90m
kube-system   ebs-csi-node-sdjbc                                           3/3     Running     0             79m
kube-system   kube-proxy-j5pbw                                             1/1     Running     0             90m
kube-system   kube-proxy-tmbjc                                             1/1     Running     0             79m
kube-system   metrics-server-59b569559b-2h7dj                              1/1     Running     0             80m
kubeflow      metadata-envoy-deployment-8588c4bd58-cmx8h                   1/1     Running     0             68m
kubeflow      metadata-grpc-deployment-7db7b94655-8sds7                    1/1     Running     4 (67m ago)   68m
kubeflow      metadata-writer-84bcf8554f-trq97                             1/1     Running     0             68m
kubeflow      ml-pipeline-7b466cb948-5xhx5                                 1/1     Running     1 (66m ago)   68m
kubeflow      ml-pipeline-persistenceagent-dc6b65b4f-x9bjq                 1/1     Running     0             68m
kubeflow      ml-pipeline-scheduledworkflow-84d5c9f555-zhwnx               1/1     Running     2 (65m ago)   68m
kubeflow      ml-pipeline-ui-5858fccc6b-nkdrm                              1/1     Running     0             68m
kubeflow      ml-pipeline-viewer-crd-74ff7b49d9-fvnbd                      1/1     Running     0             68m
kubeflow      ml-pipeline-visualizationserver-5fb6b5ccbf-hfd4c             1/1     Running     0             68m
kubeflow      mysql-7c486d86f9-g42gp                                       1/1     Running     0             68m
kubeflow      seaweedfs-758595c5d6-4pgc4                                   1/1     Running     0             68m
kubeflow      sklearn-iris-demo-84rz2-system-container-driver-2334247982   0/2     Completed   0             17m
kubeflow      sklearn-iris-demo-84rz2-system-dag-driver-1178736439         0/2     Completed   0             19m
kubeflow      workflow-controller-567d789d94-2x4s4                         1/1     Running     0             68m
```

How to read it:

* **airflow** — the whole Airflow footprint is just three pods: bundled
  Postgres, the scheduler (`3/3` = scheduler + git-sync + log-groomer
  containers), and the webserver. During a DAG run you'd also see short-lived
  task pods (`etl-simple-…`, `train-on-kubeflow-…`) that vanish when the task
  ends. A restart or two on the webserver is normal (slow first boot on small
  spot nodes).
* **kube-system** — `aws-node` (VPC CNI), `kube-proxy` and `ebs-csi-node` are
  DaemonSets: one copy per node, so with 2 nodes you see each twice; when the
  pipelines node scales up, a third copy of each appears. The rest is the
  addons this repo installs: ALB controller (1 replica), cluster-autoscaler,
  metrics-server, and the EBS CSI controller (2 replicas is that addon's
  default).
* **kubeflow** — the KFP control plane (`ml-pipeline*`, `metadata-*`,
  `workflow-controller`) plus its two backing stores: `mysql` (run history)
  and `seaweedfs` (internal artifacts). The `sklearn-iris-demo-*  Completed`
  pods are leftovers from pipeline runs — KFP v2 "driver" pods, tiny
  bookkeeping containers that plan each run; the actual executor pods are
  garbage-collected minutes after the run, the Completed drivers linger
  harmlessly for a while longer. `cache-deployer`/`cache-server` are absent
  by design (scaled to 0 — see gotchas).
* **What's *not* there** — no pods on the `pipelines` node group: it's back
  at zero. During a run you'd briefly see the `train`/`evaluate-and-publish`
  executor pods here plus a third `aws-node`/`kube-proxy`/`ebs-csi-node` set
  on the new node.

### Sidecars: why READY says 3/3

Sidecars aren't pods — they're extra containers *inside* a pod, which is what
`3/3` on the scheduler means. To see every pod's containers:

```bash
kubectl get pods -A -o custom-columns='NS:.metadata.namespace,POD:.metadata.name,CONTAINERS:.spec.containers[*].name'
```

Or only the pods that actually have companions, init containers included:

```bash
kubectl get pods -A -o json | jq -r '.items[]
  | select(((.spec.containers | length) + ((.spec.initContainers // []) | length)) > 1)
  | .metadata.namespace + "/" + .metadata.name + "\n   containers: " + ([.spec.containers[].name] | join(", "))
  + (if .spec.initContainers then "\n   init:       " + ([.spec.initContainers[].name] | join(", ")) else "" end)'
```

Real output from this stack (abridged):

```
airflow/airflow-scheduler-…
   containers: scheduler, git-sync, scheduler-log-groomer
   init:       wait-for-airflow-migrations, git-sync-init
kube-system/aws-node-…
   containers: aws-node, aws-eks-nodeagent
   init:       aws-vpc-cni-init
kube-system/ebs-csi-controller-…
   containers: ebs-plugin, csi-provisioner, csi-attacher, csi-snapshotter, csi-resizer, liveness-probe
kube-system/ebs-csi-node-…
   containers: ebs-plugin, node-driver-registrar, liveness-probe
kubeflow/sklearn-iris-demo-…-driver-…
   containers: wait, main
   init:       init
```

A short tour of the pattern: the Airflow scheduler carries `git-sync`
(continuous DAG pulls) and a log groomer; the EBS CSI controller is one real
driver (`ebs-plugin`) plus five standard CSI sidecars that each handle one
storage verb (provision/attach/snapshot/resize/probe); Argo pairs your `main`
container with a `wait` sidecar for completion + artifact bookkeeping.

Handy to know:

* A container's name is what `-c` targets everywhere:
  `kubectl logs <pod> -c git-sync`, `kubectl exec <pod> -c scheduler -- env`.
* The command counts `initContainers` on purpose: init containers are the
  run-before cousins of sidecars — and since Kubernetes 1.28, *native*
  sidecars are implemented as init containers with `restartPolicy: Always`,
  so a chart's git-sync may live in either list.

---

## 🔴 Cost kill switch (park it, don't destroy it)

Not demoing today? Scale every node group to zero and keep all state:

```bash
./scripts/kill-switch.sh off    # ~30 s → burn rate drops to ≈ $0.105/h
./scripts/kill-switch.sh on     # pods reschedule in ~5 min, state intact
```

`off` pauses the cluster-autoscaler first (otherwise it would immediately
scale back up for the pending Airflow pods), then sets both node groups to
`min=0, desired=0`. The Airflow DB and KFP MySQL/seaweedfs PVCs persist, so
DAG history and pipeline runs survive. A later `terraform apply` also acts
as "on" (it restores `min_size`; `desired_size` is lifecycle-ignored).

## Teardown

```bash
./scripts/teardown.sh           # or: make destroy
```

This is deliberately **more than** `terraform destroy`, in this order:

1. Deletes any `LoadBalancer` Services (none exist by default) — ELBs must be
   removed by Kubernetes *while the cluster is alive* or they leak.
2. Deletes the `airflow` and `kubeflow` namespaces and waits for the PVs to go —
   PVC deletion is what makes the EBS CSI driver delete the backing **EBS
   volumes**. Destroy the cluster first and those volumes are orphaned but
   still billing.
3. `terraform destroy -auto-approve` — the S3 bucket has `force_destroy = true`,
   so it's emptied and deleted (no "bucket not empty" failure, no leaked bucket).
4. Runs `scripts/cleanup-orphans.sh` as a final audit.

### Resources that can leak and keep billing

| Resource | Created by | Leak scenario | Covered by |
|---|---|---|---|
| EBS volumes (Postgres 8 Gi, MySQL 20 Gi, seaweedfs 20 Gi) | EBS CSI driver via PVCs | cluster destroyed before PVCs deleted | teardown step 2; audit via `cleanup-orphans.sh --delete` |
| ALB/NLB/classic ELB + their security groups | LB controller / Services | ingress or LB Service left at destroy time | teardown step 1 + orphan script |
| S3 bucket + objects | Terraform | `force_destroy=false` (not here) or destroy interrupted | `force_destroy=true` + orphan script |
| CloudWatch log groups `/aws/eks/<cluster>/*` | EKS if control-plane logging enabled | logging enabled manually at some point | never enabled + orphan script |
| Orphaned ENIs / EIPs | VPC teardown races | rare; NAT EIP is Terraform-managed | destroy retries; orphan script lists SGs |

`./scripts/cleanup-orphans.sh` is a **dry-run report** (also runnable anytime,
even months later, via `CLUSTER_NAME=afkf-demo-eks AWS_REGION=us-west-2 ./scripts/cleanup-orphans.sh`);
add `--delete` to remove what it finds.

---

## Design decisions & tradeoffs

**Local Terraform state (deliberate).** Remote S3 state + locking is the
correct call for anything shared or long-lived, but for a single-operator
throwaway it adds a bootstrap resource that itself can leak. The tradeoff: if
you lose the `terraform/` directory before destroying, you orphan the stack
(the orphan script + AWS console tag search `Project=eks-airflow-kubeflow-demo`
is your recovery path). A ready-to-uncomment S3 backend block (with TF ≥ 1.10
S3-native locking — no DynamoDB table needed) is in `terraform/versions.tf`.

**Public subnets, no NAT.** Nodes get public IPs; inbound is still blocked by
the cluster security group (nothing opens node ports, SSH is disabled). Saves
~$1.10/day + per-GiB processing. `enable_nat = true` flips to private subnets
behind a **single** NAT gateway if your org policy requires it.

**KFP standalone, not full Kubeflow.** The full platform (Istio, Dex, KNative,
central dashboard…) needs ~4× this cluster. Standalone KFP is the pipelines
engine only, which is exactly what the demo shows.

**Bundled Postgres, not RDS.** "Bundled" means Airflow's metadata database is
not an external AWS service but ships *inside the Helm chart* as a subchart:
a PostgreSQL container running as an ordinary pod in the cluster
(`airflow-postgresql-0` in the `airflow` namespace), its data on an 8 Gi gp3
PVC. Zero extra AWS cost beyond that volume, but no backups, no failover, and
it dies with the cluster — fine for a demo; the first thing to replace with
RDS for anything real.

**Taint on the `pipelines` group.** Guarantees only KFP executor pods (which
carry a matching toleration, added via `kfp-kubernetes` in the pipeline code)
land there, so it reliably scales back to zero. If tolerations misbehave on
some KFP version, set `enable_pipelines_taint = false` and rely on the node
selector alone.

**No custom Airflow image.** The KFP-submitting task runs in
`@task.virtualenv(requirements=["kfp==2.7.0"])` — pip installs at task runtime
(~1 min) instead of maintaining an ECR image. Right tradeoff for a demo,
wrong one for production.

## Optional: expose Airflow via ALB

Port-forwarding costs nothing; an ALB adds ~$0.60+/day — only do this if you
must share the UI. With the
ALB controller already installed:

```yaml
# add to terraform/modules/addons/values/airflow-values.yaml.tpl
ingress:
  web:
    enabled: true
    ingressClassName: alb
    annotations:
      alb.ingress.kubernetes.io/scheme: internet-facing
      alb.ingress.kubernetes.io/target-type: ip
      alb.ingress.kubernetes.io/listen-ports: '[{"HTTP": 80}]'
```

then `terraform apply`. **The ALB is created outside Terraform** — remove the
ingress (or run the teardown script, which handles it) before destroying, and
change the default `admin/admin` login (`webserver.defaultUser` in values)
before exposing anything.

## Known gotchas

* **Don't pin old KFP versions.** Google's Container Registry sunset purged
  many `gcr.io/ml-pipeline` tags, so KFP ≤ 2.5 manifests reference images
  that now 404 (their minio tag, for one). `deploy-kfp.sh` pins 2.16.1, which
  pulls from ghcr.io / Docker Hub / quay.io instead.
* **KFP's result-cache is disabled on purpose.** Its `cache-deployer` mints a
  webhook TLS cert via the Kubernetes CSR API, which EKS's signer refuses for
  non-node identities — it crashloops forever. `deploy-kfp.sh` scales the two
  cache components to zero; harmless here since the sample DAG submits runs
  with `enable_caching=False` anyway.
* **Chart RBAC must match the autoscaler image.** The cluster-autoscaler Helm
  chart ships the ClusterRole; an old chart with a new image (≥ 1.33 needs
  `volumeattachments` list/watch) leaves the autoscaler silently unable to
  scale anything. Symptom: pods Pending forever, `forbidden` spam in its logs.
* **Spot capacity errors** at node-group creation: add more instance types to
  `general_instance_types` / `pipelines_instance_types` or switch region.
* **Bitnami image purge**: Docker Hub `bitnami/postgresql` tags moved in 2025;
  the values file pins `bitnamilegacy/postgresql`. If Postgres ever
  `ErrImagePull`s, that override is the place to look.
* **First `train_on_kubeflow` run is slow** (~10 min) — venv pip install +
  scale-from-zero + image pulls. Subsequent runs ≈ 3–4 min.
* **kfp SDK 2.7 needs Python ≤ 3.12** — only relevant for recompiling the
  pipeline locally; the committed YAML works as-is.
* **Spot interruptions** can kill the scheduler or a mid-run pipeline pod at
  any time. It's a demo; everything restarts and reruns.

## Repository layout

```
eks-airflow-kubeflow-demo/
├── README.md
├── Makefile                          # deploy / pf / stop / start / destroy
├── dags/
│   ├── etl_simple.py                 # DAG 1: extract→transform→load→S3 (IRSA)
│   └── trigger_kubeflow_pipeline.py  # DAG 2: submit KFP run, wait, verify S3
├── pipelines/
│   ├── sklearn_pipeline.py           # KFP v2 source (train → evaluate+publish)
│   ├── sklearn_pipeline.yaml         # compiled package (committed; Airflow submits this)
│   └── requirements.txt              # compile-time deps only
├── scripts/
│   ├── deploy-kfp.sh                 # KFP standalone install + IRSA annotation
│   ├── port-forward.sh               # both UIs, no load balancers
│   ├── kill-switch.sh                # on|off — park nodes at zero
│   ├── teardown.sh                   # ordered destroy (PVCs → ELBs → terraform)
│   └── cleanup-orphans.sh            # post-destroy billing-leak audit [--delete]
└── terraform/
    ├── versions.tf                   # pins + local-state rationale + S3 backend snippet
    ├── providers.tf                  # aws / kubernetes / helm (EKS exec auth)
    ├── variables.tf                  # all cost knobs live here
    ├── main.tf                       # module wiring
    ├── outputs.tf
    ├── terraform.tfvars.example
    └── modules/
        ├── vpc/                      # 2 public subnets; optional private+single-NAT
        ├── eks/                      # cluster, OIDC, 2 SPOT MNGs, autoscaler ASG tags
        ├── iam/                      # 5 IRSA roles (+ vendored ALB policy JSON)
        ├── s3/                       # 1 bucket, force_destroy, lifecycle rules
        └── addons/                   # EBS CSI, gp3 SC, ALB ctrl, autoscaler, Airflow
```
