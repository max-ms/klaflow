output "ml_scoring_service" {
  value = "${kubernetes_service.ml_scoring.metadata[0].name}.${kubernetes_namespace.decisions.metadata[0].name}.svc.cluster.local:8000"
}
