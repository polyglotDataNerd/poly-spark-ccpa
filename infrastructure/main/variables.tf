variable "awsaccess" {}
variable "awssecret" {}
variable "rsuser" {}
variable "rspw" {}
variable "environment" {
  description = "env will be passed as an arguement in the build"
}

variable "region" {
  description = "Region that the instances will be created"
  default = "us-west-2"
}

variable "availability_zones" {
  type = "list"
  description = "The AZ that the resources will be launched"
  default = [
    "us-west-2a",
    "us-west-2b",
    "us-west-2c"]
}

# Networking
variable "vpc_cidr" {
  description = "The CIDR block of the VPC"
  default = "10.0.0.0/16"
}

variable "ip_cidr" {
  description = "ip CIDR range"
  type = "map"
  default = {
    public = "10.0.4.0/24,10.0.5.0/24,10.0.6.0/24"
    private = "10.0.7.0/24,10.0.8.0/24,10.0.9.0/24"
  }
}

variable "ecs_IAMROLE" {
  description = "The IAM role for the container"
  type = "string"
  default = "arn:aws:iam::447388672287:role/bigdata-dev-role"
}

variable "repository_name" {
  description = "repository name for container images"
  type = "string"
  default = "poly-spark-ccpa"
}

variable "ecr_account_path" {
  description = "ecr path for data aws account"
  type = "string"
  default = "447388672287.dkr.ecr.us-west-2.amazonaws.com"
}

variable "ecs_cluster" {
  description = "ecs clutser"
  type = "string"
  default = "data-fargate-cluster"
}

variable "private_subnets" {
  description = "generic data private subnets"
  type = "map"
  default = {
    testing = "subnet-0b96df7879c8dc10f,subnet-0df9ad59d27ff98fd,subnet-029d5e294241c83ee"
    production = "subnet-05495ba617a541515,subnet-0a87c15bfc4e4517c,subnet-0958f31b1b8ec88fa"
  }
}

variable "public_subnets" {
  description = "The private subnets to use"

  type = "map"
  default = {
    testing = "subnet-0cba3cec4da926e86,subnet-05343ed998f2bc1a7,subnet-0ea64f6e3be1aabea"
    production = "subnet-01116e90917c89682,subnet-0e366fd4cac7ca471,subnet-0c7e6328e3e3b59ce"
  }
}

variable "sg_security_groups" {
  description = "generic security groups"
  type = "map"
  default = {
    testing = "sg-0d415489a7e7de454,sg-0c08bd8a906d315c9"
    production = "sg-04bd133673c9e6436,sg-0b16ce13b03cf9fc6"
  }
}