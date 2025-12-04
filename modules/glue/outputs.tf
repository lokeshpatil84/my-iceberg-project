output "job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.iceberg_etl.name
}

output "database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.iceberg_db.name
}