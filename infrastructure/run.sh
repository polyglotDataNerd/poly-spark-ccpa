#!/usr/bin/env bash
AWS_ACCESS_KEY_ID=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/AccessKey --query Parameters[0].Value --with-decryption --output text)
AWS_SECRET_ACCESS_KEY=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/SecretKey --query Parameters[0].Value --with-decryption --output text)
UBER_ACCESS_KEY_ID=$(aws ssm get-parameters --names /CCPA/ingest/accesskey --query Parameters[0].Value --with-decryption --output text)
UBER_SECRET_ACCESS_KEY=$(aws ssm get-parameters --names /CCPA/ingest/secretkey --query Parameters[0].Value --with-decryption --output text)
GitToken=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/GitToken --query Parameters[0].Value --with-decryption --output text)
GitHash=$(cd ~/poly-spark-ccpa && (git rev-parse --verify HEAD))
EpochTag="$(date +%s)"
#shell parameter for env.
environment=$1
image="447388672287.dkr.ecr.us-west-2.amazonaws.com/sg-CCPA-$environment:$EpochTag"
TaskDef=$(cat <<-END
{
  "family": "poly-spark-ccpa-$environment",
  "networkMode": "awsvpc",
  "taskRoleArn": "arn:aws:iam::447388672287:role/bigdata-dev-role",
  "executionRoleArn": "arn:aws:iam::447388672287:role/bigdata-dev-role",
  "containerDefinitions": [
    {
      "name": "sg-CCPA-definition-$environment",
      "image": "$image",
      "essential": true,
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "poly-spark-ccpa-$environment",
          "awslogs-region": "us-west-2",
          "awslogs-stream-prefix": "CCPA-$environment"
        }
      }
    }
  ],
  "requiresCompatibilities": [
    "FARGATE"
  ],
  "memory": "30 GB",
  "cpu": "4 vCPU"
}
END
)


cd ~/poly-spark-ccpa/infrastructure/environment/build
docker build -f Dockerfile \
-t CCPA-$environment:$EpochTag -t sg-CCPA-$environment:$EpochTag \
--build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
--build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
--build-arg UBER_ACCESS_KEY_ID=$UBER_ACCESS_KEY_ID \
--build-arg UBER_SECRET_ACCESS_KEY=$UBER_SECRET_ACCESS_KEY \
--build-arg GitToken=$GitToken \
--build-arg GitHash=$GitHash \
--force-rm \
--no-cache .

docker tag CCPA-$environment:$EpochTag $image
eval "$(aws ecr get-login --region us-west-2 --no-include-email)"
docker push $image
aws ecs register-task-definition --cli-input-json "$TaskDef"



