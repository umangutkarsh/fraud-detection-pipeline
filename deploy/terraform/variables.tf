variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-2" # London
}

variable "cluster_name" {
  description = "EKS cluster name; also used as a prefix for related resources."
  type        = string
  default     = "fraud-pipeline"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.33"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT. SPOT is ~60-70% cheaper and fine for a demo."
  type        = string
  default     = "SPOT"
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_desired_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 4
}

variable "github_repository" {
  description = "GitHub repo (owner/name) allowed to deploy via OIDC."
  type        = string
  default     = "umangutkarsh/fraud-detection-pipeline"
}

variable "github_branch" {
  description = "Branch whose workflows may assume the deploy role."
  type        = string
  default     = "main"
}

variable "create_github_oidc_provider" {
  description = "Set false if your AWS account already has the GitHub OIDC provider."
  type        = bool
  default     = true
}

variable "grafana_admin_password" {
  description = "Initial Grafana admin password."
  type        = string
  sensitive   = true
}
