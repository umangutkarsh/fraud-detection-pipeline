# Prometheus Operator + Prometheus + Grafana. The app's ServiceMonitor and
# the dashboard ConfigMap (deploy/k8s) are picked up automatically.
resource "helm_release" "kube_prometheus_stack" {
  name             = "monitoring"
  namespace        = "monitoring"
  create_namespace = true

  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "^75.0.0"

  timeout = 900

  values = [yamlencode({
    prometheus = {
      prometheusSpec = {
        retention = "2d"
        # Discover ServiceMonitors in every namespace, not just this release.
        serviceMonitorSelectorNilUsesHelmValues = false
        podMonitorSelectorNilUsesHelmValues     = false
      }
    }
    grafana = {
      adminPassword = var.grafana_admin_password
      sidecar = {
        dashboards = {
          enabled         = true
          label           = "grafana_dashboard"
          searchNamespace = "ALL"
        }
      }
    }
    alertmanager = { enabled = false }
  })]

  depends_on = [module.eks]
}
