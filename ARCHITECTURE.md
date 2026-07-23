# Architecture

## [1.0.0] - 2026-07-23  


## Overview

```mermaid
flowchart LR
  subgraph Internet
    User[Browser]
    DataGouv[data.gouv.fr / OpenDataSoft]
  end

  subgraph AWS["AWS Cloud Services"]
    Route53[Route53]
    EC2[EC2 t3.micro]
    RDS[(RDS PostgreSQL)]
    S3[S3 — Let's Encrypt certs for https]

    Route53 --> EC2
    EC2 --> RDS
    EC2 --> S3
  end

  User --> Route53
  DataGouv --> EC2

   ```


Terraform Insfrastructure as code successfully automates deployment of:    
- Prod and Pre-Prod resources on AWS. 
- All application services on Prod and Pre-Prod, with use of scripts in folder scripts_ec2_deployment/ 

NB: To handle limited RAM on the chosen AWS EC2 instance, the ETL is:

- split into independent stages "extract", "insert_france2017", "insert_france2022"   
- raw data loading is split into chunks that are read sequencially   




### Production environment  
Services deployed inside an AWS ec2 instance with docker compose:  
- Flask web app  
- ETL
- PostgreSQL Database  

Additional AWS services:  
- Elastic IP to the ec2 instance  
- Route53 to map domain name to the Elastic IP  
- S3 to backup Let's Encrypt certificate files for https  
- IAM profile, policy and role to access S3 bucket from within ec2  

Source code on main branch of version control will typically be used here.


### Pre-Production environment  
The following Production resources are duplicated to create Pre-Prod:  
- EC2 instance  
- Elastic IP  
- Domain name in Route53
- S3 bucket with relevant IAM profile, policy and role 
- IAM profile, policy and role for S3 access   

Source code on dev branch of version control will typically be used here.

What would happen in production when integrating new development features is simulated here in Pre-Prod.  


### Development environment  
To verify source code on dev branch of version control successfully deploys containerised application services by themselves, it's possible to launch all 3 docker containers on any local machine.  

    docker-compose up -d db  
    docker-compose up -d datafeed  
    docker-compose up -d webapp   

This would be the Development Environment, with the Flask web service on localhost port 5000.

### Switching from Production to Pre-Production, and Development environments  

The environment variable APP_ENV = dev, staging, prod controls which is selected.  

python code in config.py sets APP_ENV to dev if no prior value is detected. 

Terraform declarations will force the value of APP_ENV to either staging or prod, depending on whether dev.tfvars or prod.tfvars is selected:  

	terraform apply -var-file="dev.tfvars"



