# prod.tfvars — hardened EXAMPLE. Point kube_context at your real cluster
# and inject the secrets from your secret manager:
#
#   export TF_VAR_jwt_secret="$(vault kv get -field=jwt secret/raven)"
#   export TF_VAR_database_url="$(vault kv get -field=database_url secret/raven)"
#   terraform -chdir=deployments/terraform apply -var-file=environments/prod.tfvars
#
# NEVER commit real values into this file.

kube_context = "" # e.g. "raven-prod" — empty means current context

release_name     = "raven"
namespace        = "raven"
create_namespace = true

values_files = [
  "../helm/raven/values-prod.yaml",
]

# Render the Secret from the injected TF_VAR_* values (never from a file
# in the repo).
create_secrets = true

# Hardened runtime knobs that belong to the environment, not the chart.
extra_set = {
  "networkPolicy.externalAccess" = "false"
  "config.wsAllowAnonymous"      = "false"
  "gateway.ingress.host"         = "raven.example.com"
}

wait            = true
timeout_seconds = 900
