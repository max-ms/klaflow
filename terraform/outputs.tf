output "kafka_namespace" {
  value = module.kafka.namespace
}

output "clickhouse_namespace" {
  value = module.clickhouse.namespace
}

output "flink_namespace" {
  value = module.flink.namespace
}

output "monitoring_namespace" {
  value = module.observability.namespace
}

output "api_namespace" {
  value = module.api.namespace
}
