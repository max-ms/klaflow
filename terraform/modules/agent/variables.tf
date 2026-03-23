variable "namespace" {
  description = "Kubernetes namespace for the agent service"
  type        = string
  default     = "agent"
}

variable "anthropic_api_key" {
  description = "Anthropic API key for the agent LLM"
  type        = string
  sensitive   = true
  default     = ""
}
