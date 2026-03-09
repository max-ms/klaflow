resource "kubernetes_namespace" "streaming" {
  metadata {
    name = "streaming"
  }
}

resource "helm_release" "kafka" {
  name       = "kafka"
  namespace  = kubernetes_namespace.streaming.metadata[0].name
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "kafka"
  version    = "28.3.0"

  timeout = 900

  # Use bitnamilegacy images (Bitnami deprecated docker.io/bitnami)
  set {
    name  = "image.repository"
    value = "bitnamilegacy/kafka"
  }

  set {
    name  = "volumePermissions.image.repository"
    value = "bitnamilegacy/os-shell"
  }

  set {
    name  = "controller.replicaCount"
    value = "1"
  }

  set {
    name  = "controller.resources.requests.memory"
    value = "512Mi"
  }

  set {
    name  = "controller.resources.requests.cpu"
    value = "250m"
  }

  set {
    name  = "controller.resources.limits.memory"
    value = "1Gi"
  }

  set {
    name  = "controller.resources.limits.cpu"
    value = "500m"
  }

  # Listeners
  set {
    name  = "listeners.client.protocol"
    value = "PLAINTEXT"
  }

  set {
    name  = "listeners.controller.protocol"
    value = "PLAINTEXT"
  }

  # Disable external access for kind
  set {
    name  = "externalAccess.enabled"
    value = "false"
  }

  # Enable auto-creation of internal topics (__consumer_offsets)
  set {
    name  = "extraConfig"
    value = "offsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1\ntransaction.state.log.min.isr=1"
  }

  # Persistence - small for local dev
  set {
    name  = "controller.persistence.size"
    value = "2Gi"
  }
}

resource "kubernetes_deployment" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = kubernetes_namespace.streaming.metadata[0].name
  }

  depends_on = [helm_release.kafka]

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "schema-registry"
      }
    }

    template {
      metadata {
        labels = {
          app = "schema-registry"
        }
      }

      spec {
        enable_service_links = false

        container {
          name  = "schema-registry"
          image = "confluentinc/cp-schema-registry:7.7.1"

          port {
            container_port = 8081
          }

          env {
            name  = "SCHEMA_REGISTRY_HOST_NAME"
            value = "schema-registry"
          }

          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"
            value = "kafka.streaming.svc.cluster.local:9092"
          }

          env {
            name  = "SCHEMA_REGISTRY_LISTENERS"
            value = "http://0.0.0.0:8081"
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "250m"
            }
          }

          startup_probe {
            http_get {
              path = "/subjects"
              port = 8081
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            failure_threshold     = 30
          }

          liveness_probe {
            http_get {
              path = "/subjects"
              port = 8081
            }
            initial_delay_seconds = 60
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/subjects"
              port = 8081
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = kubernetes_namespace.streaming.metadata[0].name
  }

  spec {
    selector = {
      app = "schema-registry"
    }

    port {
      port        = 8081
      target_port = 8081
    }
  }
}
