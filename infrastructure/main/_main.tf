provider "aws" {
  access_key = "${var.awsaccess}"
  secret_key = "${var.awssecret}"
  region = "us-west-2"
}
