# RAVEN Terraform module — deploys the RAVEN platform into a Kubernetes
# cluster via the Helm chart in deployments/helm/raven.
#
# What it manages:
#   - the target namespace (optional)
#   - one helm_release of the RAVEN chart (all services, broker, postgres,
#     redis, network policies, observability)
#
# What it deliberately does NOT manage: backups, PVC data, DNS records.
# Backups live in scripts/backup.sh; runbooks in docs/disaster-recovery.md.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.13.0"
    }
  }
}
