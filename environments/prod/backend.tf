terraform {
  backend "s3" {
    # bucket = "your-terraform-state-bucket"
    # key    = "iceberg-etl/prod/terraform.tfstate"
    # region = "ap-south-1"
    # 
    # Uncomment and configure the above for remote state
    # For now, using local state
  }
}