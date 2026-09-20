output "namespace" {
  description = "Namespace RAVEN was deployed into."
  value       = var.namespace
}

output "release_name" {
  description = "Helm release name."
  value       = helm_release.raven.name
}

output "release_status" {
  description = "Helm release status (deployed when healthy)."
  value       = helm_release.raven.status
}

output "chart_version" {
  description = "Version of the deployed RAVEN chart."
  value       = helm_release.raven.version
}

output "gateway_service_type" {
  description = "Service type of the gateway (LoadBalancer on Docker Desktop, ClusterIP behind an Ingress in prod)."
  value       = data.kubernetes_service_v1.gateway.spec[0].type
}

output "gateway_port" {
  description = "Host-facing port of the gateway Service."
  value       = data.kubernetes_service_v1.gateway.spec[0].port[0].port
}

output "gateway_url" {
  description = "How to reach the gateway. LoadBalancer on Docker Desktop binds localhost; otherwise port-forward."
  value       = data.kubernetes_service_v1.gateway.spec[0].type == "LoadBalancer" ? "http://localhost:${data.kubernetes_service_v1.gateway.spec[0].port[0].port}" : "kubectl -n ${var.namespace} port-forward svc/gateway ${data.kubernetes_service_v1.gateway.spec[0].port[0].port}:${data.kubernetes_service_v1.gateway.spec[0].port[0].port}"
}

output "console_url" {
  description = "How to reach the operations console."
  value       = data.kubernetes_service_v1.console.spec[0].type == "LoadBalancer" ? "http://localhost:${data.kubernetes_service_v1.console.spec[0].port[0].port}" : "kubectl -n ${var.namespace} port-forward svc/console ${data.kubernetes_service_v1.console.spec[0].port[0].port}:${data.kubernetes_service_v1.console.spec[0].port[0].port}"
}

output "next_steps" {
  description = "Pointers for day-2 operations."
  value       = "Backups: scripts/backup.sh -n ${var.namespace} · Runbooks: docs/disaster-recovery.md · Chart knobs: deployments/helm/raven/values.yaml"
}
