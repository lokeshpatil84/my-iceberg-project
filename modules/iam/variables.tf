variable "role_name" {
  description = "Name of the IAM role"
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the S3 bucket"
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