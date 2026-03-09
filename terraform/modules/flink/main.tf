resource "kubernetes_namespace" "processing" {
  metadata {
    name = "processing"
  }
}

resource "helm_release" "flink" {
  name       = "flink"
  namespace  = kubernetes_namespace.processing.metadata[0].name
  repository = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.10.0/"
  chart      = "flink-kubernetes-operator"
  version    = "1.10.0"

  timeout = 900

  set {
    name  = "operatorResources.requests.memory"
    value = "256Mi"
  }

  set {
    name  = "operatorResources.requests.cpu"
    value = "100m"
  }

  set {
    name  = "operatorResources.limits.memory"
    value = "512Mi"
  }

  set {
    name  = "operatorResources.limits.cpu"
    value = "250m"
  }

  set {
    name  = "webhook.create"
    value = "false"
  }
}
