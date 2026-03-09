resource "kubernetes_namespace" "ui" {
  metadata {
    name = "ui"
  }
}

resource "kubernetes_deployment" "klaflow_ui" {
  metadata {
    name      = "klaflow-ui"
    namespace = kubernetes_namespace.ui.metadata[0].name
    labels = {
      app = "klaflow-ui"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "klaflow-ui"
      }
    }

    template {
      metadata {
        labels = {
          app = "klaflow-ui"
        }
      }

      spec {
        container {
          name  = "klaflow-ui"
          image = var.image
          port {
            container_port = 80
          }
          resources {
            requests = {
              cpu    = "100m"
              memory = "64Mi"
            }
            limits = {
              cpu    = "200m"
              memory = "128Mi"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "klaflow_ui" {
  metadata {
    name      = "klaflow-ui"
    namespace = kubernetes_namespace.ui.metadata[0].name
  }

  spec {
    selector = {
      app = "klaflow-ui"
    }

    port {
      port        = 3000
      target_port = 80
    }

    type = "ClusterIP"
  }
}

variable "image" {
  description = "Docker image for the UI"
  type        = string
  default     = "klaflow-ui:latest"
}

output "service_name" {
  value = kubernetes_service.klaflow_ui.metadata[0].name
}

output "namespace" {
  value = kubernetes_namespace.ui.metadata[0].name
}
