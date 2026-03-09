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
  source     = "./modules/api"
  depends_on = [module.clickhouse, module.redis]
}
