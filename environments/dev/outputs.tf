output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = module.s3.bucket_name
}

output "glue_job_name" {
  description = "Name of the Glue job"
  value       = module.glue.job_name
}

output "glue_database_name" {
  description = "Name of the Glue database"
  value       = module.glue.database_name
}

output "iam_role_arn" {
  description = "ARN of the IAM role"
  value       = module.iam.role_arn
}