variable "kubeconfig_path" {
  description = "Path to the kubeconfig both providers authenticate with."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Kubeconfig context to use. Empty = current context (docker-desktop on a local setup)."
  type        = string
  default     = ""
}

variable "release_name" {
  description = "Helm release name for the RAVEN deployment."
  type        = string
  default     = "raven"
}

variable "namespace" {
  description = "Kubernetes namespace to deploy RAVEN into."
  type        = string
  default     = "raven"
}

variable "create_namespace" {
  description = "Create the namespace via Terraform. Set false when deploying into an existing one."
  type        = bool
  default     = true
}

variable "chart_path" {
  description = "Path to the RAVEN Helm chart. Defaults to the chart inside this repo."
  type        = string
  default     = ""
}

variable "values_files" {
  description = "Ordered list of chart values files (later wins). Environment .tfvars set this."
  type        = list(string)
  default     = []
}

variable "create_secrets" {
  description = "Let the chart render raven-secrets from the injected sensitive values."
  type        = bool
  default     = true
}

variable "jwt_secret" {
  description = "JWT signing secret. Inject via TF_VAR_jwt_secret or a gitignored .tfvars — never commit it."
  type        = string
  default     = ""
  sensitive   = true
}

variable "jwt_secret_previous" {
  description = "Previous JWT secret — set ONLY during a rotation window (docs/security/rotation.md)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "database_url" {
  description = "Postgres connection string for RAVEN services."
  type        = string
  default     = ""
  sensitive   = true
}

variable "extra_set" {
  description = "Extra chart overrides as dotted-key -> string-value pairs."
  type        = map(string)
  default     = {}
}

variable "wait" {
  description = "Wait for the release to become ready during apply."
  type        = bool
  default     = true
}

variable "timeout_seconds" {
  description = "Helm wait timeout in seconds."
  type        = number
  default     = 600
}
