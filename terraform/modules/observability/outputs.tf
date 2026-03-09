output "namespace" {
  value = kubernetes_namespace.monitoring.metadata[0].name
}

output "prometheus_url" {
  value = "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
}

output "grafana_url" {
  value = "http://kube-prometheus-stack-grafana.monitoring.svc.cluster.local:80"
}
