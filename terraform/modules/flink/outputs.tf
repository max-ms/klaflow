output "namespace" {
  value = kubernetes_namespace.processing.metadata[0].name
}
