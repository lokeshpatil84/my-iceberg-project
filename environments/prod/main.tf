terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  bucket_name = "${var.project_name}-${var.environment}-${random_id.bucket_suffix.hex}"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

module "s3" {
  source = "../../modules/s3"
  
  bucket_name = local.bucket_name
  environment = var.environment
  tags        = local.common_tags
}

module "iam" {
  source = "../../modules/iam"
  
  role_name   = "${var.project_name}-${var.environment}-glue-role"
  bucket_arn  = module.s3.bucket_arn
  environment = var.environment
  tags        = local.common_tags
}

resource "aws_s3_object" "glue_script" {
  bucket = module.s3.bucket_name
  key    = "scripts/glue_job.py"
  source = "../../etl/glue_job.py"
  etag   = filemd5("../../etl/glue_job.py")
}

module "glue" {
  source = "../../modules/glue"
  
  job_name        = "${var.project_name}-${var.environment}-etl-job"
  role_arn        = module.iam.role_arn
  script_location = "s3://${module.s3.bucket_name}/scripts/glue_job.py"
  environment     = var.environment
  tags            = local.common_tags
  
  depends_on = [aws_s3_object.glue_script]
}