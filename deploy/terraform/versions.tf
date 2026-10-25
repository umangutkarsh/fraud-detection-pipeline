terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.17"
    }
  }

  # Remote state. Create the bucket once (see README), then uncomment.
  # backend "s3" {
  #   bucket       = "<your-tfstate-bucket>"
  #   key          = "fraud-pipeline/terraform.tfstate"
  #   region       = "eu-west-2"
  #   use_lockfile = true
  # }
}
