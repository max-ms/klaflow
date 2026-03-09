resource "kubernetes_namespace" "decisions" {
  metadata {
    name = "decisions"
  }
}

# ML Scoring Service deployment
resource "kubernetes_deployment" "ml_scoring" {
  metadata {
    name      = "ml-scoring"
    namespace = kubernetes_namespace.decisions.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "ml-scoring"
      }
    }

    template {
      metadata {
        labels = {
          app = "ml-scoring"
        }
      }

      spec {
        container {
          name  = "ml-scoring"
          image = "klaflow-ml-models:latest"
          image_pull_policy = "Never"

          port {
            container_port = 8000
          }

          resources {
            requests = {
              memory = "128Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "256Mi"
              cpu    = "200m"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "ml_scoring" {
  metadata {
    name      = "ml-scoring"
    namespace = kubernetes_namespace.decisions.metadata[0].name
  }

  spec {
    selector = {
      app = "ml-scoring"
    }

    port {
      port        = 8000
      target_port = 8000
    }
  }
}

# Reward Logger deployment
resource "kubernetes_deployment" "reward_logger" {
  metadata {
    name      = "reward-logger"
    namespace = kubernetes_namespace.decisions.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "reward-logger"
      }
    }

    template {
      metadata {
        labels = {
          app = "reward-logger"
        }
      }

      spec {
        container {
          name  = "reward-logger"
          image = "klaflow-decision-engine:latest"
          image_pull_policy = "Never"

          env {
            name  = "CLICKHOUSE_HOST"
            value = "clickhouse.analytics.svc.cluster.local"
          }

          resources {
            requests = {
              memory = "128Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "256Mi"
              cpu    = "200m"
            }
          }
        }
      }
    }
  }
}
