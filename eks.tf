# ---------------------------------------------------------------------------
# EKS: the runtime for the live stack (Kafka, Flink, Airflow, Redis, API).
# Off by default; nothing runs idle. Turn on with -var enable_eks=true.
#
# Network: default VPC, public subnets, no NAT gateway and no load balancer.
# Nodes get public IPs so they reach ECR, S3 and the EKS API directly;
# their security group still allows no inbound traffic from the internet.
# Access to services is via kubectl port-forward.
# ---------------------------------------------------------------------------

variable "enable_eks" {
  description = "Create the EKS cluster and its node group."
  type        = bool
  default     = false
}

variable "eks_kubernetes_version" {
  description = "Kubernetes version of the cluster (keep it in standard support; extended support costs 6x)."
  type        = string
  default     = "1.37"
}

variable "eks_node_instance_type" {
  description = "Instance type of the worker nodes."
  type        = string
  default     = "m7i.xlarge"
}

variable "eks_node_count" {
  description = "Number of worker nodes when the node group is created. Later scaling is done with the AWS CLI (0 when idle)."
  type        = number
  default     = 2
}

variable "eks_node_az" {
  description = "All nodes share one availability zone: EBS volumes (Kafka, Airflow disks) cannot move between zones."
  type        = string
  default     = "us-east-1a"
}

locals {
  eks_name = "${var.project}-eks"

  # The control plane needs subnets in at least two zones; us-east-1e is not
  # supported by EKS, so it is left out.
  eks_control_plane_azs = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"]
}

data "aws_subnets" "eks_control_plane" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }

  filter {
    name   = "availability-zone"
    values = local.eks_control_plane_azs
  }
}

data "aws_subnets" "eks_nodes" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }

  filter {
    name   = "availability-zone"
    values = [var.eks_node_az]
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.28.1"

  create = var.enable_eks

  name               = local.eks_name
  kubernetes_version = var.eks_kubernetes_version

  vpc_id     = data.aws_vpc.default.id
  subnet_ids = data.aws_subnets.eks_control_plane.ids

  # The API endpoint is public but needs IAM authentication; kubectl runs
  # from a laptop without a VPN or bastion.
  endpoint_public_access = true

  # Access entries (API) instead of the old aws-auth ConfigMap; whoever runs
  # terraform becomes cluster admin.
  authentication_mode                      = "API"
  enable_cluster_creator_admin_permissions = true

  # Cost: no customer KMS key (EKS already encrypts Kubernetes secrets with an
  # AWS-owned key) and no control-plane logs in CloudWatch (audit logs are
  # billed per GB ingested). Turn logs on when debugging the control plane.
  create_kms_key              = false
  encryption_config           = null
  enabled_log_types           = []
  create_cloudwatch_log_group = false

  addons = {
    # Networking: every pod gets a real VPC IP address.
    vpc-cni = {
      before_compute = true
    }
    kube-proxy = {}
    coredns    = {}

    # Lets pods assume IAM roles (one role per service).
    eks-pod-identity-agent = {
      before_compute = true
    }

    # Creates EBS volumes for Kafka, Airflow and Prometheus disks.
    aws-ebs-csi-driver = {
      pod_identity_association = [{
        role_arn        = one(aws_iam_role.ebs_csi[*].arn)
        service_account = "ebs-csi-controller-sa"
      }]
    }
  }

  eks_managed_node_groups = {
    main = {
      instance_types = [var.eks_node_instance_type]
      ami_type       = "AL2023_x86_64_STANDARD"
      subnet_ids     = data.aws_subnets.eks_nodes.ids

      # min 0 so the group can be scaled to zero when idle; max 3 leaves room
      # for one more node without a Terraform change.
      min_size     = 0
      max_size     = 3
      desired_size = var.eks_node_count

      # EKS's own launch template: disk_size only works this way.
      use_custom_launch_template = false
      disk_size                  = 50
    }
  }
}

# ---------------------------------------------------------------------------
# IAM role for the EBS CSI driver, assumed through EKS Pod Identity.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "pod_identity_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  count = var.enable_eks ? 1 : 0

  name               = "${local.eks_name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  count = var.enable_eks ? 1 : 0

  role       = aws_iam_role.ebs_csi[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

output "eks_cluster_name" {
  value = var.enable_eks ? module.eks.cluster_name : null
}

output "eks_kubeconfig" {
  description = "Adds the cluster to ~/.kube/config."
  value       = var.enable_eks ? "aws eks update-kubeconfig --name ${local.eks_name} --region ${var.region}" : null
}
