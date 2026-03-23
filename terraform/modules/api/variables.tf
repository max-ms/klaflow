variable "namespace" {
  description = "Kubernetes namespace for the API"
  type        = string
  default     = "api"
}

variable "anthropic_api_key" {
  description = "Anthropic API key for the AI agent"
  type        = string
  default     = ""
  sensitive   = true
}
