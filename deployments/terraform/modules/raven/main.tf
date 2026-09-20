# The namespace RAVEN lives in. All chart objects use the release
# namespace, so this is the only kubernetes_* resource the module needs;
# everything else goes through the Helm chart (single source of truth for
# the workload definitions).
resource "kubernetes_namespace_v1" "raven" {
  count = var.create_namespace ? 1 : 0

  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of"    = "raven"
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
}

# helm provider 3.x takes `set` as a list attribute (no nested blocks), so
# every override is assembled here. Conditional entries only appear when
# their variable is non-empty.
locals {
  release_set = concat(
    [{ name = "secrets.create", value = tostring(var.create_secrets) }],
    var.jwt_secret != "" ? [{ name = "secrets.jwtSecret", value = var.jwt_secret }] : [],
    var.jwt_secret_previous != "" ? [{ name = "secrets.jwtSecretPrevious", value = var.jwt_secret_previous }] : [],
    var.database_url != "" ? [{ name = "secrets.databaseUrl", value = var.database_url }] : [],
    [for k, v in var.extra_set : { name = k, value = v }],
  )
}

resource "helm_release" "raven" {
  name      = var.release_name
  namespace = var.namespace
  chart     = var.chart_path
  values    = [for f in var.values_files : file(f)]
  set       = local.release_set
  wait      = var.wait
  timeout   = var.timeout_seconds
  # The namespace (and any secrets you pre-created in it) must exist first.
  depends_on = [kubernetes_namespace_v1.raven]
}

# Surface the gateway Service so outputs can report how to reach the
# platform without hard-coding anything.
data "kubernetes_service_v1" "gateway" {
  metadata {
    name      = "gateway"
    namespace = var.namespace
  }
  depends_on = [helm_release.raven]
}

data "kubernetes_service_v1" "console" {
  metadata {
    name      = "console"
    namespace = var.namespace
  }
  depends_on = [helm_release.raven]
}
