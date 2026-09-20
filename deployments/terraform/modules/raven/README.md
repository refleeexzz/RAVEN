# raven module

Deploys the RAVEN platform into a Kubernetes cluster through the Helm chart
at `deployments/helm/raven`. Owns the namespace (optionally) and one
`helm_release`; everything workload-shaped stays in the chart.

## Usage

```hcl
module "raven" {
  source = "./modules/raven"

  namespace    = "raven"
  chart_path   = "${path.root}/../helm/raven"
  values_files = ["${path.root}/../helm/raven/values-dev.yaml"]

  create_secrets = true
  jwt_secret     = var.jwt_secret     # sensitive, inject via TF_VAR or .tfvars
  database_url   = var.database_url

  extra_set = {
    "worker.replicas" = "8"
  }
}
```

## Inputs

See `variables.tf` — every variable carries a description. The big ones:
`chart_path`, `values_files`, `jwt_secret` / `database_url` (sensitive),
`extra_set`, `wait` / `timeout_seconds`.

## Outputs

Namespace, release name/status/chart version, gateway + console URLs
(localhost links on Docker Desktop, port-forward hints otherwise).

## Notes

- Providers (`kubernetes`, `helm`) are configured by the ROOT module, not
  here — this module only declares the versions it needs.
- Secrets flow in as sensitive variables; they never land in this file.
- The module does not manage PVC data — restores are a runbook, not a
  `terraform apply` (see docs/disaster-recovery.md).
