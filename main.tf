terraform {
  required_version = ">= 1.17.0"

  # State lives in S3 (bucket defined in state.tf), versioned and encrypted.
  # use_lockfile: a lock object next to the state stops two applies from
  # writing at once (S3-native locking, no DynamoDB table needed).
  backend "s3" {
    bucket       = "nyc-taxi-tfstate-969788248726"
    key          = "nyc-taxi/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }
}

# ---------------------------------------------------------------------------
# Variables
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

variable "enable_compute" {
  description = "Create the EC2 instance and its IAM/SG/alarm resources. When off, only S3 is created."
  type        = bool
  default     = false
}

variable "instance_type" {
  description = "Instance type of the server that runs all containers."
  type        = string
  default     = "m7i.xlarge"
}

variable "root_volume_gb" {
  description = "Root volume size (GB)."
  type        = number
  default     = 100
}

variable "idle_cpu_threshold" {
  description = "The instance counts as idle while average CPU stays below this percentage."
  type        = number
  default     = 5
}

variable "idle_minutes" {
  description = "Stop the instance after this many idle minutes (must be a multiple of 5)."
  type        = number
  default     = 60

  validation {
    condition     = var.idle_minutes >= 5 && var.idle_minutes % 5 == 0
    error_message = "idle_minutes must be a multiple of 5 (CloudWatch basic monitoring uses 5-minute periods)."
  }
}

# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  compute_count = var.enable_compute ? 1 : 0
}

# ---------------------------------------------------------------------------
# S3: raw data bucket (all public access blocked)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  # Bucket names are globally unique; the account ID avoids collisions.
  bucket = "${var.project}-raw-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

data "aws_iam_policy_document" "raw_tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "raw" {
  bucket = aws_s3_bucket.raw.id
  policy = data.aws_iam_policy_document.raw_tls_only.json

  # Apply the public access block first, or the policy may be rejected as "public".
  depends_on = [aws_s3_bucket_public_access_block.raw]
}

# ---------------------------------------------------------------------------
# IAM: EC2 role (this bucket + SSM only)
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  count = local.compute_count

  name               = "${var.project}-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  count = local.compute_count

  role       = aws_iam_role.ec2[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "raw_bucket" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]
  }

  statement {
    sid = "ReadWriteObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
}

resource "aws_iam_role_policy" "raw_bucket" {
  count = local.compute_count

  name   = "raw-bucket-access"
  role   = aws_iam_role.ec2[0].id
  policy = data.aws_iam_policy_document.raw_bucket.json
}

resource "aws_iam_instance_profile" "ec2" {
  count = local.compute_count

  name = "${var.project}-ec2"
  role = aws_iam_role.ec2[0].name
}

# ---------------------------------------------------------------------------
# Network: default VPC, security group with no inbound access
# ---------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

# Not every AZ offers every instance type (e.g. us-east-1e); keep the ones that do.
data "aws_ec2_instance_type_offerings" "supported" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

data "aws_subnets" "default" {
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
    values = data.aws_ec2_instance_type_offerings.supported.locations
  }
}

resource "aws_security_group" "ec2" {
  count = local.compute_count

  name        = "${var.project}-ec2"
  description = "No inbound ports; access via SSM. Outbound open (SSM, Docker, downloads)."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# EC2: single instance, Amazon Linux 2023, Docker at boot
# ---------------------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_instance" "main" {
  count = local.compute_count

  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids      = [aws_security_group.ec2[0].id]
  iam_instance_profile        = aws_iam_instance_profile.ec2[0].name
  associate_public_ip_address = true # reach SSM without a NAT gateway or VPC endpoints

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 2          # let containers reach the role credentials too
  }

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    dnf install -y docker
    systemctl enable --now docker
    usermod -aG docker ec2-user

    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
  EOF

  tags = {
    Name = "${var.project}-main"
  }

  lifecycle {
    # Don't rebuild the instance when a new AMI is released.
    ignore_changes = [ami]
  }
}

# ---------------------------------------------------------------------------
# Idle stop: stop the instance when CPU stays low
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "idle_stop" {
  count = local.compute_count

  alarm_name        = "${var.project}-idle-stop"
  alarm_description = "Stops the EC2 instance when CPU stays below ${var.idle_cpu_threshold}% for ${var.idle_minutes} minutes."

  namespace   = "AWS/EC2"
  metric_name = "CPUUtilization"
  dimensions = {
    InstanceId = aws_instance.main[0].id
  }

  statistic           = "Average"
  period              = 300
  evaluation_periods  = var.idle_minutes / 5
  threshold           = var.idle_cpu_threshold
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "missing" # no data while stopped; keep the current state

  alarm_actions = ["arn:aws:automate:${var.region}:ec2:stop"]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "instance_id" {
  value = one(aws_instance.main[*].id)
}

output "ssm_connect" {
  description = "Connect to the instance (needs session-manager-plugin)."
  value       = var.enable_compute ? "aws ssm start-session --target ${aws_instance.main[0].id} --region ${var.region}" : null
}
