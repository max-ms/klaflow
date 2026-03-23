module "kafka" {
  source = "./modules/kafka"
}

module "clickhouse" {
  source = "./modules/clickhouse"
}

module "flink" {
  source = "./modules/flink"
}

module "observability" {
  source = "./modules/observability"
}

module "redis" {
  source = "./modules/redis"
}

module "decisions" {
  source     = "./modules/decisions"
  depends_on = [module.clickhouse, module.redis]
}

module "api" {
  source            = "./modules/api"
  anthropic_api_key = var.anthropic_api_key
  depends_on        = [module.clickhouse, module.redis]
}

module "agent" {
  source     = "./modules/agent"
  depends_on = [module.api]
}

module "streaming_consumers" {
  source     = "./modules/streaming_consumers"
  depends_on = [module.flink, module.kafka, module.clickhouse]
}

module "feature_writer" {
  source     = "./modules/feature_writer"
  depends_on = [module.flink, module.clickhouse, module.redis, module.decisions]
}

module "segment_worker" {
  source     = "./modules/segment_worker"
  depends_on = [module.flink, module.clickhouse]
}
