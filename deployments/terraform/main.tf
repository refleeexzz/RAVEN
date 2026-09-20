# Root module: wire the raven module to the values of this environment.

locals {
  # Default chart location: this repo's deployments/helm/raven.
  chart_path = var.chart_path != "" ? var.chart_path : abspath("${path.module}/../helm/raven")
}

module "raven" {
  source = "./modules/raven"

  release_name     = var.release_name
  namespace        = var.namespace
  create_namespace = var.create_namespace

  chart_path   = local.chart_path
  values_files = var.values_files

  create_secrets      = var.create_secrets
  jwt_secret          = var.jwt_secret
  jwt_secret_previous = var.jwt_secret_previous
  database_url        = var.database_url

  extra_set       = var.extra_set
  wait            = var.wait
  timeout_seconds = var.timeout_seconds
}
