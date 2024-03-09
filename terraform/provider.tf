provider "aws" {
    region = "eu-west-2"
}

terraform {
  
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "5.7.0"
    }
  }
  
  backend "s3" {
    bucket = "de-project-nessians-terraform-backend-bucket"
    key = "tfstate"
    region = "eu-west-2"  
  }
}