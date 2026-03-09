output "namespace" {
  value = kubernetes_namespace.analytics.metadata[0].name
}

output "host" {
  value = "clickhouse.analytics.svc.cluster.local"
}

output "port" {
  value = "8123"
}

output "native_port" {
  value = "9000"
}
