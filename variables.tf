# ---------------------------------------------------------------------------
# Project-wide variables (each component keeps its own next to it)
# ---------------------------------------------------------------------------

variable "project" {
  description = "Project name used in resource names and tags."
  type        = string
  default     = "nyc-taxi"
}

variable "region" {
  description = "AWS region. The NYC TLC open data is in us-east-1 too, so transfer is free."
  type        = string
  default     = "us-east-1"
}
