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
