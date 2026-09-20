output "namespace" {
  description = "Namespace RAVEN was deployed into."
  value       = module.raven.namespace
}

output "release_status" {
  description = "Helm release status."
  value       = module.raven.release_status
}

output "chart_version" {
  description = "Deployed RAVEN chart version."
  value       = module.raven.chart_version
}

output "gateway_url" {
  description = "How to reach the gateway (localhost URL on Docker Desktop, port-forward hint otherwise)."
  value       = module.raven.gateway_url
}

output "console_url" {
  description = "How to reach the operations console."
  value       = module.raven.console_url
}

output "next_steps" {
  description = "Day-2 pointers (backups, runbooks, chart knobs)."
  value       = module.raven.next_steps
}
