# Convenience wrapper — every target is also runnable by hand (see README).
TF := terraform -chdir=terraform

.PHONY: init plan apply kubeconfig kfp deploy pipeline pf stop start destroy orphans volumes pvc inventory nodegroups pods deployments images s3 dags workflows sidecars pdbs force-drain irsa iam git-sync

init:        ## terraform init
	$(TF) init

plan:        ## terraform plan
	$(TF) plan

apply:       ## provision VPC + EKS + addons + Airflow (~20 min)
	$(TF) apply

kubeconfig:  ## point kubectl at the new cluster
	aws eks update-kubeconfig --region "$$($(TF) output -raw region)" --name "$$($(TF) output -raw cluster_name)"

kfp:         ## install Kubeflow Pipelines standalone
	./scripts/deploy-kfp.sh

deploy: apply kubeconfig kfp  ## full deployment, end to end

# kfp 2.7 supports python <=3.12 only; 'python3' may be newer (and Homebrew's
# refuses bare 'pip install' anyway, PEP 668) — so compile in a local venv
# built from the newest usable interpreter on PATH.
PIPELINE_PY := $(shell command -v python3.12 || command -v python3.11 || command -v python3.10 || command -v python3)

pipeline:    ## recompile pipelines/sklearn_pipeline.yaml in pipelines/.venv (kfp needs python <=3.12)
	cd pipelines && $(PIPELINE_PY) -m venv .venv \
	  && .venv/bin/pip install -q -r requirements.txt \
	  && .venv/bin/python sklearn_pipeline.py

pf:          ## port-forward both UIs (Airflow :8080, KFP :8081)
	./scripts/port-forward.sh

stop:        ## cost kill switch: scale all node groups to 0
	./scripts/kill-switch.sh off

start:       ## undo the kill switch
	./scripts/kill-switch.sh on

destroy:     ## ordered teardown + orphan report
	./scripts/teardown.sh

orphans:     ## list (not delete) leaked billable resources
	./scripts/cleanup-orphans.sh

volumes:     ## EBS volumes provisioned by the cluster's CSI driver (PVC-backed — these bill while parked)
	@{ echo "ID AZ GiB TYPE STATE CREATED PVC"; \
	aws ec2 describe-volumes --region "$$($(TF) output -raw region)" \
	  --filters "Name=tag:kubernetes.io/cluster/$$($(TF) output -raw cluster_name),Values=owned" \
	  --query 'Volumes[].[VolumeId,AvailabilityZone,Size,VolumeType,State,CreateTime,Tags[?Key==`kubernetes.io/created-for/pvc/name`]|[0].Value]' \
	  --output text; } | sed -E 's/(T[0-9]{2}:[0-9]{2})[^[:space:]]*/\1/g' | column -t

pvc:         ## PVCs → bound volume's AZ, flagged STRANDED when no live node is there (k8s mirror of 'volumes')
	@./scripts/pvc-status.sh

inventory:   ## every AWS resource carrying the Terraform default Project tag (CSI volumes NOT included — see 'volumes')
	aws resourcegroupstaggingapi get-resources --region "$$($(TF) output -raw region)" \
	  --tag-filters Key=Project,Values=eks-airflow-kubeflow-demo \
	  --query 'ResourceTagMappingList[].ResourceARN' --output table

pods:        ## every pod in the cluster, with the node it runs on (see README: "What a healthy system looks like")
	kubectl get pods -A -o wide

deployments: ## Deployments + StatefulSets rollout state — watch 'make kfp' / 'make start' converge
	@kubectl get deploy,sts -A
	@echo ""
	@echo "Still rolling out (READY below desired):"
	@kubectl get deploy,sts -A --no-headers 2>/dev/null \
	  | awk '{split($$3,r,"/"); if (r[1] != r[2]) print "  " $$1 "  " $$2 "  " $$3}' \
	  | grep . || echo "  none — everything is fully rolled out"

images:      ## container images actually running, dedup'd with container counts — the stack's live bill of materials
	@{ echo "CONTAINERS IMAGE"; \
	kubectl get pods -A -o json | jq -r '.items[].spec | (.containers + (.initContainers // []))[].image' \
	  | sort | uniq -c | sort -rn | awk '{print $$1 " " $$2}'; } | column -t

s3:          ## everything in the demo bucket (task logs, ETL output, published models) + totals
	aws s3 ls --recursive --human-readable --summarize \
	  "s3://$$($(TF) output -raw s3_bucket)/"

dags:        ## Airflow DAGs (paused state) + each one's 3 most recent runs with durations
	@./scripts/dag-runs.sh

git-sync:    ## did my DAG change reach Airflow? local vs GitHub vs cluster-synced commit
	@./scripts/git-sync-status.sh

workflows:   ## KFP runs as Argo Workflow objects, oldest first (one per pipeline run)
	kubectl -n kubeflow get workflows --sort-by=.metadata.creationTimestamp

pdbs:        ## PodDisruptionBudgets + draining nodes — ALLOWED DISRUPTIONS 0 = drains stall there (kill-switch's lingering last node)
	kubectl get pdb -A
	@echo ""
	@echo "Nodes currently draining (cordoned):"
	@kubectl get nodes --no-headers 2>/dev/null | grep SchedulingDisabled \
	  || echo "  none"
	@echo ""
	@echo "PDBs with zero budget right now (evictions of their pods will be refused):"
	@kubectl get pdb -A --no-headers 2>/dev/null | awk '$$5 == 0 {print "  " $$1 "/" $$2}' \
	  | grep . || echo "  none — drains can proceed"

force-drain: ## unstick PDB-blocked node drains: delete non-DaemonSet pods on cordoned nodes (bypasses PDBs)
	@./scripts/force-drain.sh

iam:         ## project IAM roles: trusted principals + attached policies (AWS-side mirror of 'irsa')
	@./scripts/iam-roles.sh

irsa:        ## service accounts annotated with IAM roles — the cluster's entire AWS-access wiring
	@{ echo "NAMESPACE SERVICEACCOUNT IAM_ROLE"; \
	kubectl get sa -A -o json | jq -r '.items[] \
	  | select(.metadata.annotations["eks.amazonaws.com/role-arn"]) \
	  | .metadata.namespace + " " + .metadata.name + " " \
	    + (.metadata.annotations["eks.amazonaws.com/role-arn"] | sub(".*role/"; ""))'; } | column -t
	@echo ""
	@echo "(pods using an unlisted SA have NO AWS access — e.g. a missing kubeflow/pipeline-runner"
	@echo " row means deploy-kfp.sh's annotation step didn't run and pipelines can't reach S3)"

sidecars:    ## pods with >1 container: sidecars + init containers by name (see README: "Sidecars")
	@kubectl get pods -A -o json | jq -r '.items[] \
	  | select(((.spec.containers | length) + ((.spec.initContainers // []) | length)) > 1) \
	  | .metadata.namespace + "/" + .metadata.name \
	    + "\n   containers: " + ([.spec.containers[].name] | join(", ")) \
	    + (if .spec.initContainers then "\n   init:       " + ([.spec.initContainers[].name] | join(", ")) else "" end)'

nodegroups:  ## node groups (scaling, eligible AZs) + live nodes with their actual AZ
	@{ echo "NAME STATUS CAPACITY TYPES MIN DESIRED MAX CREATED ELIGIBLE_AZS"; \
	REGION="$$($(TF) output -raw region)"; CLUSTER="$$($(TF) output -raw cluster_name)"; \
	aws eks list-nodegroups --region "$$REGION" --cluster-name "$$CLUSTER" \
	    --query 'nodegroups[]' --output text | tr '\t' '\n' | while read -r ng; do \
	  row="$$(aws eks describe-nodegroup --region "$$REGION" --cluster-name "$$CLUSTER" --nodegroup-name "$$ng" \
	    --query 'nodegroup.[nodegroupName,status,capacityType,join(`,`,instanceTypes),scalingConfig.minSize,scalingConfig.desiredSize,scalingConfig.maxSize,createdAt]' \
	    --output text)"; \
	  subnets="$$(aws eks describe-nodegroup --region "$$REGION" --cluster-name "$$CLUSTER" --nodegroup-name "$$ng" \
	    --query 'nodegroup.subnets' --output text)"; \
	  azs="$$(aws ec2 describe-subnets --region "$$REGION" --subnet-ids $$subnets \
	    --query 'Subnets[].AvailabilityZone' --output text | tr '\t' ',')"; \
	  echo "$$row $$azs"; \
	done; } | sed -E 's/(T[0-9]{2}:[0-9]{2})[^[:space:]]*/\1/g' | column -t
	@echo ""
	@echo "Live nodes (actual AZ — spot placement can pile into one AZ, see README):"
	@kubectl get nodes -o custom-columns='NAME:.metadata.name,INSTANCE:.spec.providerID,ZONE:.metadata.labels.topology\.kubernetes\.io/zone,WORKLOAD:.metadata.labels.workload,JOINED:.metadata.creationTimestamp,READY:.status.conditions[-1].type' 2>/dev/null \
	  | sed -E -e 's|aws:///[a-z0-9-]*/||' -e 's/(T[0-9]{2}:[0-9]{2})[^[:space:]]*/\1/g' | column -t \
	  || echo "  (cluster unreachable or zero nodes)"
