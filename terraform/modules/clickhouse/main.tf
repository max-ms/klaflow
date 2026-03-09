resource "kubernetes_namespace" "analytics" {
  metadata {
    name = "analytics"
  }
}

resource "helm_release" "clickhouse" {
  name       = "clickhouse"
  namespace  = kubernetes_namespace.analytics.metadata[0].name
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "clickhouse"
  version    = "9.4.4"

  timeout = 900

  # Use bitnamilegacy images (Bitnami deprecated docker.io/bitnami)
  set {
    name  = "image.repository"
    value = "bitnamilegacy/clickhouse"
  }

  set {
    name  = "keeper.image.repository"
    value = "bitnamilegacy/clickhouse-keeper"
  }

  set {
    name  = "volumePermissions.image.repository"
    value = "bitnamilegacy/os-shell"
  }

  set {
    name  = "shards"
    value = "1"
  }

  set {
    name  = "replicaCount"
    value = "1"
  }

  set {
    name  = "resources.requests.memory"
    value = "1Gi"
  }

  set {
    name  = "resources.requests.cpu"
    value = "500m"
  }

  set {
    name  = "resources.limits.memory"
    value = "2Gi"
  }

  set {
    name  = "resources.limits.cpu"
    value = "1"
  }

  set {
    name  = "persistence.size"
    value = "2Gi"
  }

  set {
    name  = "auth.username"
    value = "klaflow"
  }

  set_sensitive {
    name  = "auth.password"
    value = "klaflow-pass"
  }

  # Keeper for coordination
  set {
    name  = "keeper.enabled"
    value = "true"
  }
}

resource "kubernetes_config_map" "clickhouse_schema" {
  metadata {
    name      = "clickhouse-schema"
    namespace = kubernetes_namespace.analytics.metadata[0].name
  }

  data = {
    "schema.sql" = file("${path.module}/../../../src/clickhouse/schema.sql")
  }
}
