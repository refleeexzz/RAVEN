variable "release_name" {
  description = "Name of the Helm release (and helm list entry) for RAVEN."
  type        = string
  default     = "raven"
}

variable "namespace" {
  description = "Kubernetes namespace RAVEN is deployed into. Created when create_namespace is true."
  type        = string
  default     = "raven"
}

variable "create_namespace" {
  description = "Whether the module should create (and own) the namespace. Set false if it already exists."
  type        = bool
  default     = true
}

variable "chart_path" {
  description = "Filesystem path to the RAVEN Helm chart (deployments/helm/raven in the repo)."
  type        = string
}

variable "values_files" {
  description = "Ordered list of values YAML files applied to the release (later files win), e.g. values-dev.yaml."
  type        = list(string)
  default     = []
}

variable "jwt_secret" {
  description = "JWT signing secret injected as secrets.jwtSecret. Keep empty to skip (chart then expects an existing Secret)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "jwt_secret_previous" {
  description = "Previous JWT secret, set ONLY during a rotation window (see docs/security/rotation.md)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "database_url" {
  description = "Postgres connection string injected as secrets.databaseUrl. Keep empty to skip."
  type        = string
  default     = ""
  sensitive   = true
}

variable "create_secrets" {
  description = "When true the chart renders raven-secrets from jwt_secret/database_url. When false it expects a pre-existing Secret."
  type        = bool
  default     = true
}

variable "extra_set" {
  description = "Extra chart values as a map of dotted keys to string values (translated to helm set blocks), e.g. { \"worker.replicas\" = \"8\" }."
  type        = map(string)
  default     = {}
}

variable "wait" {
  description = "Wait for all release resources to become ready before marking the apply done."
  type        = bool
  default     = true
}

variable "timeout_seconds" {
  description = "Helm wait timeout in seconds. The migrate hook + image pulls can take a while on cold clusters."
  type        = number
  default     = 600
}
