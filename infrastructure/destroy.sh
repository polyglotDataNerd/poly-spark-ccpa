#!/usr/bin/env bash
AWS_ACCESS_KEY_ID=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/AccessKey --query Parameters[0].Value --with-decryption --output text)
AWS_SECRET_ACCESS_KEY=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/SecretKey --query Parameters[0].Value --with-decryption --output text)
CURRENTDATE="$(date  +%Y)"
#shell parameter for env.
environment=$1


#copy tfstate files into dir
aws s3 cp s3://polyglotDataNerd-bigdata-utility/terraform/CCPA/$environment/$CURRENTDATE ~/poly-spark-ccpa/infrastructure/main  --recursive --sse --quiet --include "*"

export TF_VAR_awsaccess=$AWS_ACCESS_KEY_ID
export TF_VAR_awssecret=$AWS_SECRET_ACCESS_KEY
export TF_VAR_environment=$environment

cd ~/poly-spark-ccpa/infrastructure/main
terraform init
terraform destroy -auto-approve

#copy tfstate files to s3
aws s3 cp ~/poly-spark-ccpa/infrastructure/main/ s3://polyglotDataNerd-bigdata-utility/terraform/CCPA/environment/$CURRENTDATE/  --recursive --sse --quiet --exclude "*" --include "*terraform.tfstate*"



