# Convenience wrapper — every target is also runnable by hand (see README).
TF := terraform -chdir=terraform

.PHONY: init plan apply kubeconfig kfp deploy pipeline pf stop start destroy orphans volumes inventory nodegroups pods s3

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

pipeline:    ## recompile pipelines/sklearn_pipeline.yaml (needs python <=3.12)
	cd pipelines && python3 -m pip install -q -r requirements.txt && python3 sklearn_pipeline.py

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
	aws ec2 describe-volumes --region "$$($(TF) output -raw region)" \
	  --filters "Name=tag:kubernetes.io/cluster/$$($(TF) output -raw cluster_name),Values=owned" \
	  --query 'Volumes[].{ID:VolumeId,AZ:AvailabilityZone,GiB:Size,Type:VolumeType,State:State,PVC:Tags[?Key==`kubernetes.io/created-for/pvc/name`]|[0].Value}' \
	  --output table

inventory:   ## every AWS resource carrying the Terraform default Project tag (CSI volumes NOT included — see 'volumes')
	aws resourcegroupstaggingapi get-resources --region "$$($(TF) output -raw region)" \
	  --tag-filters Key=Project,Values=eks-airflow-kubeflow-demo \
	  --query 'ResourceTagMappingList[].ResourceARN' --output table

pods:        ## every pod in the cluster, with the node it runs on (see README: "What a healthy system looks like")
	kubectl get pods -A -o wide

s3:          ## everything in the demo bucket (task logs, ETL output, published models) + totals
	aws s3 ls --recursive --human-readable --summarize \
	  "s3://$$($(TF) output -raw s3_bucket)/"

nodegroups:  ## node groups (scaling, eligible AZs) + live nodes with their actual AZ
	@{ echo "NAME STATUS CAPACITY TYPES MIN DESIRED MAX ELIGIBLE_AZS"; \
	REGION="$$($(TF) output -raw region)"; CLUSTER="$$($(TF) output -raw cluster_name)"; \
	aws eks list-nodegroups --region "$$REGION" --cluster-name "$$CLUSTER" \
	    --query 'nodegroups[]' --output text | tr '\t' '\n' | while read -r ng; do \
	  row="$$(aws eks describe-nodegroup --region "$$REGION" --cluster-name "$$CLUSTER" --nodegroup-name "$$ng" \
	    --query 'nodegroup.[nodegroupName,status,capacityType,join(`,`,instanceTypes),scalingConfig.minSize,scalingConfig.desiredSize,scalingConfig.maxSize]' \
	    --output text)"; \
	  subnets="$$(aws eks describe-nodegroup --region "$$REGION" --cluster-name "$$CLUSTER" --nodegroup-name "$$ng" \
	    --query 'nodegroup.subnets' --output text)"; \
	  azs="$$(aws ec2 describe-subnets --region "$$REGION" --subnet-ids $$subnets \
	    --query 'Subnets[].AvailabilityZone' --output text | tr '\t' ',')"; \
	  echo "$$row $$azs"; \
	done; } | column -t
	@echo ""
	@echo "Live nodes (actual AZ — spot placement can pile into one AZ, see README):"
	@kubectl get nodes -L topology.kubernetes.io/zone,workload 2>/dev/null || echo "  (cluster unreachable or zero nodes)"
