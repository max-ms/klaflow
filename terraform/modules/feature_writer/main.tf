# Continuous feature writer — polls ClickHouse for updated counters,
# computes derived features, calls ML scoring, writes to Redis.

resource "kubernetes_deployment" "feature_writer" {
  metadata {
    name      = "feature-writer"
    namespace = "processing"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "feature-writer"
      }
    }

    template {
      metadata {
        labels = {
          app = "feature-writer"
        }
      }

      spec {
        container {
          name              = "feature-writer"
          image             = "klaflow-feature-store:latest"
          image_pull_policy = "Never"

          env {
            name  = "CLICKHOUSE_HOST"
            value = "clickhouse.analytics.svc.cluster.local"
          }
          env {
            name  = "CLICKHOUSE_PORT"
            value = "8123"
          }
          env {
            name  = "CLICKHOUSE_USER"
            value = "klaflow"
          }
          env {
            name  = "CLICKHOUSE_PASSWORD"
            value = "klaflow-pass"
          }
          env {
            name  = "REDIS_HOST"
            value = "redis-master.features.svc.cluster.local"
          }
          env {
            name  = "REDIS_PORT"
            value = "6379"
          }
          env {
            name  = "ML_SCORING_URL"
            value = "http://ml-scoring.decisions.svc.cluster.local:8000"
          }

          resources {
            requests = {
              memory = "128Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
          }
        }
      }
    }
  }
}
