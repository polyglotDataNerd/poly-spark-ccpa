variable "environment" {}

variable "rsuser" {
  description = "Catalog Master User"
}
variable "rspw" {
  description = "Catalog Master PW"
}

variable "availability_zones" {
  type = "list"
  description = "The azs to use"
}

variable "ecs_IAMROLE" {
  description = "The IAM role for the container"
  type = "string"
}


variable "repository_name" {
  description = "repository name for container images"
  type = "string"
}

variable "ecs_cluster" {
  description = "repository name for container images"
  type = "string"
}

variable "ecr_account_path" {
  description = "ecr path for data aws account"
  type = "string"
}

variable "region" {
  description = "Region that the instances will be created"
}

variable "private_subnets" {
  description = "generic data private subnets"
  type = "map"
}

variable "public_subnets" {
  description = "The private subnets to use"
  type = "map"
}

variable "sg_security_groups" {
  description = "generic security groups"
  type = "map"
}

variable "ip_cidr" {
  description = "ip CIDR range"
  type = "map"
}
