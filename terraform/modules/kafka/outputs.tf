output "namespace" {
  value = kubernetes_namespace.streaming.metadata[0].name
}

output "bootstrap_server" {
  value = "kafka.streaming.svc.cluster.local:9092"
}

output "schema_registry_url" {
  value = "http://schema-registry.streaming.svc.cluster.local:8081"
}
