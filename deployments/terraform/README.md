# RAVEN on Terraform

Terraform root module that deploys the RAVEN platform into any Kubernetes
cluster through the Helm chart in `deployments/helm/raven`. The chart is
the single source of truth for workloads; Terraform owns the namespace,
the release, and the environment-specific inputs.

## Layout

```
deployments/terraform/
├── main.tf / variables.tf / outputs.tf   # root module (wires the raven module)
├── providers.tf / versions.tf            # kubernetes + helm providers via kubeconfig
├── environments/
│   ├── dev.tfvars                        # Docker Desktop local profile
│   └── prod.tfvars                       # hardened example (inject secrets!)
└── modules/raven/                        # namespace + helm_release module
```

## Quick start (Docker Desktop)

```bash
terraform -chdir=deployments/terraform init
terraform -chdir=deployments/terraform plan  -var-file=environments/dev.tfvars
terraform -chdir=deployments/terraform apply -var-file=environments/dev.tfvars
terraform -chdir=deployments/terraform output gateway_url
```

## Prod-ish flow

```bash
export TF_VAR_jwt_secret="..."      # from your secret manager
export TF_VAR_database_url="..."
terraform -chdir=deployments/terraform apply -var-file=environments/prod.tfvars
```

## What Terraform is NOT for here

- Restoring backup data into PVCs — that is a runbook (docs/disaster-recovery.md).
- Editing individual Deployments/Services — change the chart, re-apply.
- Docker image builds — build/tag before applying (the chart pins tags).

Full docs: [docs/terraform.md](../../docs/terraform.md).
