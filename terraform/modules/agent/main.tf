resource "kubernetes_namespace" "agent" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "agent_credentials" {
  metadata {
    name      = "agent-credentials"
    namespace = kubernetes_namespace.agent.metadata[0].name
  }

  data = {
    ANTHROPIC_API_KEY = var.anthropic_api_key
    KLAFLOW_API_URL   = "http://klaflow-api.api.svc.cluster.local:80"
  }
}

resource "kubernetes_deployment" "agent" {
  metadata {
    name      = "klaflow-agent"
    namespace = kubernetes_namespace.agent.metadata[0].name
    labels = {
      app = "klaflow-agent"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "klaflow-agent"
      }
    }

    template {
      metadata {
        labels = {
          app = "klaflow-agent"
        }
      }

      spec {
        container {
          name              = "agent"
          image             = "klaflow-agent:latest"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 8001
          }

          env_from {
            secret_ref {
              name = kubernetes_secret.agent_credentials.metadata[0].name
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

resource "kubernetes_service" "agent" {
  metadata {
    name      = "klaflow-agent"
    namespace = kubernetes_namespace.agent.metadata[0].name
  }

  spec {
    selector = {
      app = "klaflow-agent"
    }

    port {
      port        = 80
      target_port = 8001
    }

    type = "ClusterIP"
  }
}
