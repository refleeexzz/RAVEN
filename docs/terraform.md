# RAVEN on Terraform

`deployments/terraform/` is a thin, honest Terraform layer over the Helm
chart. The chart stays the single source of truth for every workload;
Terraform owns the three things Helm is bad at: the namespace lifecycle,
environment-specific inputs (especially secrets), and a plan/apply audit
trail.

If you are looking for the knobs, they live in the chart
(`docs/helm.md`). If you are looking for *who creates what and when*,
you are in the right file.

---

## Layout

```
deployments/terraform/
├── versions.tf                 # terraform >= 1.5, kubernetes >= 2.30, helm >= 3.0
├── providers.tf                # both providers read your kubeconfig
├── variables.tf                # every input, described
├── main.tf                     # root module -> module "raven"
├── outputs.tf                  # urls + day-2 pointers
├── environments/
│   ├── dev.tfvars              # Docker Desktop profile
│   └── prod.tfvars             # hardened example (inject secrets!)
└── modules/raven/
    ├── main.tf                 # kubernetes_namespace_v1 + helm_release
    ├── variables.tf            # module inputs, described
    ├── outputs.tf              # namespace, release status, urls
    ├── versions.tf
    └── README.md
```

---

## Quick start (Docker Desktop)

```bash
terraform -chdir=deployments/terraform init
terraform -chdir=deployments/terraform plan  -var-file=environments/dev.tfvars
terraform -chdir=deployments/terraform apply -var-file=environments/dev.tfvars

terraform -chdir=deployments/terraform output gateway_url
terraform -chdir=deployments/terraform output console_url
```

Dev uses the chart's `values-dev.yaml`, which creates the Secret with the
documented local credentials — zero extra input needed.

Tear down:

```bash
terraform -chdir=deployments/terraform destroy -var-file=environments/dev.tfvars
# hook jobs are not tracked by the release:
kubectl delete job migrate -n raven --ignore-not-found
```

---

## How it fits together

```
you -> terraform plan/apply
        ├─ kubernetes_namespace_v1.raven      (optional, create_namespace)
        └─ helm_release.raven                 (chart: ../helm/raven)
             ├─ values_files[]                (environment profile, ordered)
             └─ set = [...]                   (secrets + extra_set, assembled in locals)
```

- `values_files` are read with `file()` and passed in order — later files
  override earlier ones, exactly like repeated `helm -f`.
- `set` entries are assembled in `locals.release_set`: `secrets.create`,
  then conditional `jwtSecret` / `jwtSecretPrevious` / `databaseUrl` (only
  when non-empty), then everything in `extra_set`.
- After the release exists, two `kubernetes_service_v1` data sources read
  the gateway and console Services so `terraform output` can tell you how
  to reach the platform without hard-coding anything.

### Providers

Both providers authenticate with your kubeconfig (`kubeconfig_path`,
`kube_context` — empty context = current one, `docker-desktop` locally).
Pin: `helm >= 3.0` because provider 3.x made `kubernetes` an argument and
`set` a list attribute; the module is written for that syntax.

---

## Secrets

Never in `.tfvars` that get committed. The flow is environment variables:

```bash
export TF_VAR_jwt_secret="$(vault kv get -field=jwt secret/raven)"
export TF_VAR_database_url="$(vault kv get -field=database_url secret/raven)"
terraform -chdir=deployments/terraform apply -var-file=environments/prod.tfvars
```

Both variables are `sensitive = true`: masked in plans, but remember they
**do** land in the Terraform state — treat the state file as a secret too
(remote backend with encryption for anything shared).

---

## Outputs

| Output | What you get |
|---|---|
| `namespace` | where RAVEN landed |
| `release_status` | `deployed` when healthy |
| `chart_version` | chart version actually running |
| `gateway_url` | `http://localhost:8080` on Docker Desktop, port-forward hint otherwise |
| `console_url` | same idea for the console |
| `next_steps` | backup + runbook pointers |

---

## What Terraform is NOT for here

- **Restoring data into PVCs.** That is a runbook
  (`docs/disaster-recovery.md`), not an `apply`. Terraform can recreate
  empty volumes; it will not pg_restore for you.
- **Editing individual Deployments or Services.** Change the chart,
  re-apply. Do not patch around the chart with `kubernetes_manifest`
  snowflakes — that is how drift is born.
- **Building images.** Tags are pinned in the chart; build and tag first,
  then apply.
- **Backup scheduling.** `scripts/backup.sh` plus your favorite scheduler.

---

## Validated

This module was run end-to-end against the local Docker Desktop cluster:
`fmt -check`, `init` (kubernetes 3.2.1 / helm 3.3.0), `validate`, `plan`
(2 to add), `apply` (namespace + release, 14 pods Ready, gateway
`/health` OK), and `destroy` (clean). One environment quirk, same as
helm.exe: run terraform from a path without spaces or em-dashes — the
repo path `F:\RAVEN — ...` is fine for editing, but copy the tree to
`%TEMP%` for actual terraform runs on this machine.
