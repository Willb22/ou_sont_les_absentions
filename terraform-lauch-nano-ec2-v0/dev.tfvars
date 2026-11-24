# Run bash deploy.sh to deploy, wait for the web app to respond, and then destroy
# the builder EC2 instance through Terraform. builder_enabled stays false here
# so a later manual terraform apply will not recreate the builder.

aws_region          = "eu-west-3"
git_repository      = "https://github.com/Willb22/ou_sont_les_absentions.git"
git_branch          = "dev"
app_env             = "staging"
database_name       = "dev_ou_sont_les_abstentions"
postgres_username   = "postgres"
postgres_secret_arn = "arn:aws:secretsmanager:eu-west-3:632279305326:secret:postgres_password-ZBCA3F"
root_volume_size_gb = 20
