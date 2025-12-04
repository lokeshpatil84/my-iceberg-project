variable "job_name" {
  description = "Name of the Glue job"
  type        = string
}

variable "role_arn" {
  description = "ARN of the IAM role for Glue"
  type        = string
}

variable "script_location" {
  description = "S3 location of the Glue script"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}