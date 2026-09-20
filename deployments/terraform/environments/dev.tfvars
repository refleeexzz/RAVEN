# dev.tfvars — local Docker Desktop environment.
#
#   terraform -chdir=deployments/terraform init
#   terraform -chdir=deployments/terraform apply -var-file=environments/dev.tfvars
#
# Uses the chart's dev profile (chart creates the Secret with documented
# DEV credentials), so a fresh laptop needs zero extra input.

release_name     = "raven"
namespace        = "raven"
create_namespace = true

values_files = [
  "../helm/raven/values-dev.yaml",
]

# dev: the chart's values-dev.yaml already carries the documented local
# credentials, so nothing sensitive is injected from Terraform here.
create_secrets = true

wait            = true
timeout_seconds = 600
