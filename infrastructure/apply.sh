#!/usr/bin/env bash
AWS_ACCESS_KEY_ID=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/AccessKey --query Parameters[0].Value --with-decryption --output text)
AWS_SECRET_ACCESS_KEY=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/SecretKey --query Parameters[0].Value --with-decryption --output text)
Catalog_USER=$(aws ssm get-parameters --names /datacatalog/DATABASE_USER --query Parameters[0].Value --with-decryption --output text)
Catalog_PW=$(aws ssm get-parameters --names /datacatalog/DATABASE_PASSWORD --query Parameters[0].Value --with-decryption --output text)
CURRENTDATE="$(date  +%Y)"
#shell parameter for env.
environment=$1

#copy tfstate files into dir
aws s3 cp s3://polyglotDataNerd-bigdata-utility/terraform/CCPA/$environment/$CURRENTDATE ~/poly-spark-ccpa/infrastructure/main  --recursive --sse --quiet --include "*"

#terraform variables
export TF_VAR_awsaccess=$AWS_ACCESS_KEY_ID
export TF_VAR_awssecret=$AWS_SECRET_ACCESS_KEY
export TF_VAR_rsuser=$Catalog_USER
export TF_VAR_rspw=$Catalog_PW
export TF_VAR_environment=$environment


#runs terraform ecs to build infrastructure. does not have to be ran every all the time
#can be re-used to register task definition
cd ~/poly-spark-ccpa/infrastructure/main
terraform init
terraform get
terraform plan
terraform apply -auto-approve

#copy tfstate files to s3
aws s3 cp ~/poly-spark-ccpa/infrastructure/main/ s3://polyglotDataNerd-bigdata-utility/terraform/CCPA/$environment/$CURRENTDATE/  --recursive --sse --quiet --exclude "*" --include "*terraform.tfstate*"

