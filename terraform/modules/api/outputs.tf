output "namespace" {
  value = kubernetes_namespace.api.metadata[0].name
}

output "service_url" {
  value = "http://klaflow-api.api.svc.cluster.local:80"
}
