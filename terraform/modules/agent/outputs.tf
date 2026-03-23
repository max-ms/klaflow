output "namespace" {
  value = kubernetes_namespace.agent.metadata[0].name
}

output "service_url" {
  value = "http://klaflow-agent.agent.svc.cluster.local:80"
}
