resource "kubernetes_namespace" "features" {
  metadata {
    name = "features"
  }
}

resource "helm_release" "redis" {
  name       = "redis"
  namespace  = kubernetes_namespace.features.metadata[0].name
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "redis"
  version    = "20.6.1"

  timeout = 600

  # Use bitnamilegacy images (Bitnami deprecated docker.io/bitnami)
  set {
    name  = "image.repository"
    value = "bitnamilegacy/redis"
  }

  set {
    name  = "global.security.allowInsecureImages"
    value = "true"
  }

  # Standalone mode (no sentinel/cluster)
  set {
    name  = "architecture"
    value = "standalone"
  }

  # Disable auth for local dev
  set {
    name  = "auth.enabled"
    value = "false"
  }

  # Master resources
  set {
    name  = "master.resources.requests.memory"
    value = "256Mi"
  }

  set {
    name  = "master.resources.limits.memory"
    value = "512Mi"
  }

  # Persistence
  set {
    name  = "master.persistence.size"
    value = "1Gi"
  }
}
