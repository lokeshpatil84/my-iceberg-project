resource "aws_glue_catalog_database" "iceberg_db" {
  name = "${var.environment}-iceberg-db"
  
  catalog_id = data.aws_caller_identity.current.account_id
}

resource "aws_glue_job" "iceberg_etl" {
  name         = var.job_name
  role_arn     = var.role_arn
  glue_version = "4.0"
  
  command {
    script_location = var.script_location
    python_version  = "3"
  }
  
  default_arguments = {
    "--enable-metrics"                = ""
    "--enable-spark-ui"              = "true"
    "--spark-event-logs-path"        = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}/sparkHistoryLogs/"
    "--enable-job-insights"          = "false"
    "--enable-observability-metrics" = "true"
    "--enable-glue-datacatalog"      = "true"
    "--job-language"                 = "python"
    "--additional-python-modules"    = "pyiceberg"
    "--conf"                         = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
  
  max_retries      = 1
  timeout          = 60
  worker_type      = "G.1X"
  number_of_workers = 2
  
  tags = merge(var.tags, {
    Environment = var.environment
  })
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}