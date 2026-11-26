SHELL := /bin/bash
PYTHON := python3
include .env_local

# Helper
# ---------


help:  ## List all the recipies of Makefile
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | column -t -s '#'
.PHONY: help



ssh_ec2_builder: ## Connect to production ec2 where the webservice is deployed
	@ssh -v -i ssh_key_eu_west_3.pem ubuntu@$(EC2_BUILDER)
.PHONY: ssh_ec2_builder

ssh_ec2_runner: ## Connect to development ec2 where the webservice is deployed
	@ssh -v -i ssh_key_eu_west_3.pem ubuntu@$(EC2_RUNNER)
.PHONY: ssh_ec2_runner

ssh_ec2_dev: ## Connect to development ec2 where the webservice is deployed
	@ssh -v -i ssh_key_eu_west_3.pem ubuntu@$(DEV_EC2)
.PHONY: ssh_ec2_dev

ssh_prod_ec2:
	@ssh -v -i ssh_key_eu_west_3.pem ubuntu@$(PROD_EC2)
.PHONY: ssh_prod_ec2

webhook_dev: ## trigger webhook from outside GitHub
	@curl -X POST 'http://$(DEV_EC2_IP)/update_server'
.PHONY: webhook_dev

deploy_db:
	@docker compose up -d db
.PHONY: deploy_db

etl_france2017:
	@docker compose -f docker-compose.yml -f docker-compose.local.yml run -d --name datafeed_france2017 datafeed --stage france2017
.PHONY: etl_france2017

etl_france2022:
	@docker compose -f docker-compose.yml -f docker-compose.local.yml run -d --name datafeed_france2022 datafeed --stage france2022
.PHONY: etl_france2022

etl_extract:
	@docker compose run -d datafeed --stage extract
.PHONY: etl_extract

etl_all:
	@docker compose run -d datafeed --stage all
.PHONY: etl_extract

deploy_etl: deploy_db etl_all

terraform_workspace_dev:
	@terraform -chdir=$(TERRAFORM_DIR) workspace select dev
.PHONY: terraform_workspace_dev

terraform_workspace_prod:
	@terraform -chdir=$(TERRAFORM_DIR) workspace select prod
.PHONY: terraform_workspace_prod

terraform_init:
	@terraform -chdir=$(TERRAFORM_DIR) init
.PHONY: terraform_init

terraform_taint_ec2:
	@terraform -chdir=$(TERRAFORM_DIR) taint aws_instance.ubuntu_ec2
.PHONY: terraform_taint_ec2

terraform_plan:
	@terraform -chdir=$(TERRAFORM_DIR) plan
.PHONY: terraform_plan

plan_dev:
	@terraform -chdir=$(TERRAFORM_DIR) plan -var-file="dev.tfvars"
.PHONY: plan_dev

terraform_plan_dev: terraform_workspace_dev plan_dev

plan_prod:
	@terraform -chdir=$(TERRAFORM_DIR) plan -var-file="prod.tfvars"
.PHONY: plan_prod

terraform_plan_prod: terraform_workspace_prod plan_prod

deploy_dev:
	@terraform -chdir=$(TERRAFORM_DIR) apply -var-file="dev.tfvars" -auto-approve
.PHONY: deploy_dev

terraform_deploy_dev: terraform_workspace_dev deploy_dev

deploy_prod:
	@terraform -chdir=$(TERRAFORM_DIR) apply -var-file="prod.tfvars" -auto-approve
.PHONY: deploy_prod

terraform_deploy_prod: terraform_workspace_prod deploy_prod
