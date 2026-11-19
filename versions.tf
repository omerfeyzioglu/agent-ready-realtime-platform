terraform {
  required_version = ">= 1.17.0"

  # State lives in S3 (bucket defined in state.tf), versioned and encrypted.
  # use_lockfile: a lock object next to the state stops two applies from
  # writing at once (S3-native locking, no DynamoDB table needed).
  backend "s3" {
    bucket       = "nyc-taxi-tfstate-969788248726"
    key          = "nyc-taxi/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }
}

# ---------------------------------------------------------------------------
# AWS provider: one region, default tags on every resource
# ---------------------------------------------------------------------------

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
