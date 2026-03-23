# Continuous segment evaluation worker — SIP two-phase pattern.
# Polls for recently changed profiles, re-evaluates segment conditions.

resource "kubernetes_deployment" "segment_worker" {
  metadata {
    name      = "segment-worker"
    namespace = "processing"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "segment-worker"
      }
    }

    template {
      metadata {
        labels = {
          app = "segment-worker"
        }
      }

      spec {
        container {
          name              = "segment-worker"
          image             = "klaflow-segment-worker:latest"
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
            name  = "POLL_INTERVAL_SECONDS"
            value = "60"
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
