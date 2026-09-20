terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 3.0.0"
    }
  }

  # Local state is fine for a single-operator dev setup. For a team, move
  # to a remote backend (S3 + lock, GCS, Terraform Cloud, ...):
  # backend "s3" { ... }
}
