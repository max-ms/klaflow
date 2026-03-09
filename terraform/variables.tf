variable "cluster_name" {
  description = "Name of the kind cluster"
  type        = string
  default     = "klaflow-us"
}

variable "kubeconfig_path" {
  description = "Path to kubeconfig file"
  type        = string
  default     = "~/.kube/config"
}

variable "kubeconfig_context" {
  description = "Kubeconfig context to use"
  type        = string
  default     = "kind-klaflow-us"
}
