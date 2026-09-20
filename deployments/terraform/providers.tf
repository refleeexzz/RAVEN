# Both providers read the same kubeconfig. Defaults match a Docker Desktop
# setup (current context); override kubeconfig_path / kube_context for
# anything else.
provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kube_context
}

provider "helm" {
  kubernetes = {
    config_path    = var.kubeconfig_path
    config_context = var.kube_context
  }
}
