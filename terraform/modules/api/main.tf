resource "kubernetes_namespace" "api" {
  metadata {
    name = "api"
  }
}

resource "kubernetes_secret" "clickhouse_credentials" {
  metadata {
    name      = "clickhouse-credentials"
    namespace = kubernetes_namespace.api.metadata[0].name
  }

  data = {
    CLICKHOUSE_HOST     = "clickhouse.analytics.svc.cluster.local"
    CLICKHOUSE_PORT     = "8123"
    CLICKHOUSE_USER     = "klaflow"
    CLICKHOUSE_PASSWORD = "klaflow-pass"
    CLICKHOUSE_DB       = "default"
  }
}

resource "kubernetes_deployment" "api" {
  metadata {
    name      = "klaflow-api"
    namespace = kubernetes_namespace.api.metadata[0].name
    labels = {
      app = "klaflow-api"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "klaflow-api"
      }
    }

    template {
      metadata {
        labels = {
          app = "klaflow-api"
        }
      }

      spec {
        container {
          name  = "api"
          image = "klaflow-api:latest"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 8000
          }

          env_from {
            secret_ref {
              name = kubernetes_secret.clickhouse_credentials.metadata[0].name
            }
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

resource "kubernetes_service" "api" {
  metadata {
    name      = "klaflow-api"
    namespace = kubernetes_namespace.api.metadata[0].name
  }

  spec {
    selector = {
      app = "klaflow-api"
    }

    port {
      port        = 80
      target_port = 8000
    }

    type = "ClusterIP"
  }
}
