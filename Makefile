# Convenience wrapper — every target is also runnable by hand (see README).
TF := terraform -chdir=terraform

.PHONY: init plan apply kubeconfig kfp deploy pipeline pf stop start destroy orphans

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
