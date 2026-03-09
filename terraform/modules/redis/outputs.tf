output "namespace" {
  value = kubernetes_namespace.features.metadata[0].name
}

output "redis_host" {
  value = "redis-master.features.svc.cluster.local"
}

output "redis_port" {
  value = 6379
}

output "redis_url" {
  value = "redis://redis-master.features.svc.cluster.local:6379"
}
