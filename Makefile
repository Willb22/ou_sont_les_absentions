SHELL := /bin/bash
PYTHON := python3
include .env

# Helper
# ---------


help:  ## List all the recipies of Makefile
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | column -t -s '#'
.PHONY: help



ssh_prod_ec2: ## Connect to production ec2 where the webservice is deployed
	@ssh -v -i aws_rsa_key_pair_2025_05_20.pem ubuntu@$(PROD_EC2)
.PHONY: ssh_prod_ec2

ssh_dev_ec2: ## Connect to development ec2 where the webservice is deployed
	@ssh -v -i aws_rsa_key_pair_2025_05_20.pem ubuntu@$(DEV_EC2)
.PHONY: ssh_dev_ec2

ssh_trial_ec2:
	@ssh -v -i aws_rsa_key_pair_2025_05_20.pem ubuntu@$(TRIAL_EC2)
.PHONY: ssh_trial_ec2


ssh_ec2_no_eip:
	@ssh -v -i aws_rsa_key_pair_2025_05_20.pem ubuntu@$(TRIAL_EC2_NO_EIP)
.PHONY: ssh_ec2_no_eip

webhook_dev: ## trigger webhook from outside GitHub
	@curl -X POST 'http://$(DEV_EC2_IP)/update_server'
.PHONY: webhook_dev

deploy_db:
	@docker compose up -d db
.PHONY: deploy_db

etl_france2017:
	@docker compose run -d datafeed --stage france2017
.PHONY: etl_france2017

etl_france2022:
	@docker compose run -d datafeed --stage france2022
.PHONY: etl_france2022

etl_extract:
	@docker compose run -d datafeed --stage extract
.PHONY: etl_extract

etl_all:
	@docker compose run -d datafeed --stage all
.PHONY: etl_extract

deploy_etl: deploy_db etl_all