/*====
This terraform build can only run once if environments persist. The container builds will be ran in a different process.
We can use the apply command to rebuild and the destroy command to delete all the environments in terraform
======*/
/*
  vpc
*/

data "aws_vpc" sg-vpc {
  tags {
    Name = "sg-vpc-${var.environment}"
  }
}

data "aws_security_group" sg-securitygroup {
  name = "bd-security-group-dbs-${var.environment}"
  vpc_id = "${data.aws_vpc.sg-vpc.id}"
  tags {
    Name = "bd-security-group-dbs-${var.environment}"
  }
}


resource "aws_db_subnet_group" "sg-rds-subnet-group" {
  name = "sg-rds-subnet-group-catalog-${var.environment}"
  subnet_ids = [
    "${split(",", var.private_subnets[var.environment])}"]
  tags = {
    Name = "sg-rds-subnet-group-catalog-${var.environment}"
    Environment = "${var.environment}"
  }
}

resource "aws_rds_cluster_parameter_group" "sg-rds-param-group" {
  name = "sg-rds-param-group-catalog-${var.environment}"
  family = "aurora-mysql5.7"
  description = "RDS production param group cluster parameter group for Data catalog"

  parameter {
    name = "character_set_server"
    value = "utf8"
  }

  parameter {
    name = "character_set_client"
    value = "utf8"
  }

  parameter {
    name = "aurora_load_from_s3_role"
    value = "arn:aws:iam::447388672287:role/sg-RDS-s3-loader"
  }

  parameter {
    name = "aurora_select_into_s3_role"
    value = "arn:aws:iam::447388672287:role/sg-RDS-s3-loader"
  }

  parameter {
    name = "aws_default_s3_role"
    value = "arn:aws:iam::447388672287:role/sg-RDS-s3-loader"
  }
  tags = {
    Name = "sg-rds-param-group-catalog-${var.environment}"
    Environment = "${var.environment}"
  }
}

#Data catalog
resource "aws_rds_cluster" "db_datacatalog" {
  cluster_identifier = "sg-bigdata-catalog-${var.environment}"
  engine = "aurora-mysql"
  engine_version = "5.7.mysql_aurora.2.03.2"
  availability_zones = [
    "${var.availability_zones}"]
  database_name = "datacatalog"
  master_username = "${var.rsuser}"
  master_password = "${var.rspw}"
  //snapshot_identifier = "${var.rds_os_snapshot_identifier}"
  db_cluster_parameter_group_name = "${aws_rds_cluster_parameter_group.sg-rds-param-group.name}"
  db_subnet_group_name = "${aws_db_subnet_group.sg-rds-subnet-group.name}"
  vpc_security_group_ids = [
    "${data.aws_security_group.sg-securitygroup.id}"]
  iam_roles = [
    "arn:aws:iam::447388672287:role/sg-RDS-s3-loader"]
  backup_retention_period = 5
  preferred_backup_window = "07:00-09:00"
  skip_final_snapshot = true
  storage_encrypted = true
  enabled_cloudwatch_logs_exports = [
    "audit",
    "error",
    "general",
    "slowquery"]
  tags {
    Name = "sg-bigdata-catalog-${var.environment}"
    Environment = "${var.environment}"
  }
}

resource "aws_rds_cluster_instance" "db_catalog_instance" {
  count = "2"
  identifier = "sg-bigdata-catalog-inst-${var.environment}-${count.index}"
  engine = "aurora-mysql"
  engine_version = "5.7.mysql_aurora.2.03.2"
  cluster_identifier = "${aws_rds_cluster.db_datacatalog.id}"
  instance_class = "db.r5.large"
  db_subnet_group_name = "${aws_db_subnet_group.sg-rds-subnet-group.name}"
  publicly_accessible = false
  //performance_insights_enabled = true

  tags {
    Name = "sg-bigdata-catalog-${var.environment}-${count.index}"
    VPC = "${data.aws_vpc.sg-vpc.id}"
    ManagedBy = "terraform"
    Environment = "${var.environment}"
  }

  lifecycle {
    create_before_destroy = false
  }

}

//provider "mysql" {
//  endpoint = "${aws_rds_cluster_instance.db_catalog_instance.endpoint}"
//  username = "${aws_rds_cluster.db_datacatalog.master_username}"
//  password = "${aws_rds_cluster.db_datacatalog.master_password  }"
//}
//
//resource "mysql_database" "audit" {
//  name = "auditdb"
//}