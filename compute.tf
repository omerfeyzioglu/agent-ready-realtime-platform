# ---------------------------------------------------------------------------
# Single EC2 server (off by default; the live stack runs on EKS instead).
# Role (this bucket + SSM), no inbound ports, Docker at boot, idle stop.
# ---------------------------------------------------------------------------

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

locals {
  compute_count = var.enable_compute ? 1 : 0
}

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

# Idle stop: stop the instance when CPU stays low.
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

output "instance_id" {
  value = one(aws_instance.main[*].id)
}

output "ssm_connect" {
  description = "Connect to the instance (needs session-manager-plugin)."
  value       = var.enable_compute ? "aws ssm start-session --target ${aws_instance.main[0].id} --region ${var.region}" : null
}
