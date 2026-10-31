terraform {
  required_version = ">= 1.17.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }
}

# ---------------------------------------------------------------------------
# Değişkenler
# ---------------------------------------------------------------------------

variable "project" {
  description = "Kaynak adlarında ve etiketlerde kullanılan proje adı."
  type        = string
  default     = "nyc-taxi"
}

variable "region" {
  description = "AWS bölgesi. NYC TLC açık veri seti de us-east-1'de, transfer ücretsiz."
  type        = string
  default     = "us-east-1"
}

variable "enable_compute" {
  description = "EC2 sunucusunu ve ona bağlı IAM/SG/alarm kaynaklarını oluştur. Kapalıyken yalnızca S3 kurulur."
  type        = bool
  default     = false
}

variable "instance_type" {
  description = "Tüm container'ları çalıştıracak sunucunun tipi."
  type        = string
  default     = "m7i.xlarge"
}

variable "root_volume_gb" {
  description = "Kök disk boyutu (GB)."
  type        = number
  default     = 100
}

variable "idle_cpu_threshold" {
  description = "Ortalama CPU bu yüzdenin altında kalırsa sunucu boşta sayılır."
  type        = number
  default     = 5
}

variable "idle_minutes" {
  description = "Sunucu bu kadar dakika boşta kalınca durdurulur (5'in katı olmalı)."
  type        = number
  default     = 60

  validation {
    condition     = var.idle_minutes >= 5 && var.idle_minutes % 5 == 0
    error_message = "idle_minutes 5'in katı olmalı (CloudWatch temel izleme 5 dakikalık)."
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
# S3: ham veri deposu (public erişim tamamen kapalı)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  # Bucket adları dünya genelinde tekil olmalı; hesap ID'si çakışmayı önler.
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

  # Public access block önce uygulanmalı, yoksa politika "public" sayılıp reddedilebilir.
  depends_on = [aws_s3_bucket_public_access_block.raw]
}

# ---------------------------------------------------------------------------
# IAM: EC2 rolü (yalnızca bu bucket + SSM)
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
# Ağ: varsayılan VPC, gelen trafiğe tamamen kapalı security group
# ---------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

# Her AZ her instance tipini desteklemiyor (ör. us-east-1e); destekleyenleri bul.
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
  description = "Gelen port yok; erisim SSM ile. Giden trafik acik (SSM, Docker, veri indirme)."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Tum giden trafik"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# EC2: tek sunucu, Amazon Linux 2023, açılışta Docker
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
  associate_public_ip_address = true # NAT/VPC endpoint olmadan SSM'e ulaşmak için

  metadata_options {
    http_tokens                 = "required" # yalnızca IMDSv2
    http_put_response_hop_limit = 2          # container'lar da rol kimlik bilgilerini alabilsin
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
    # Yeni AMI çıktığında sunucu baştan kurulmasın.
    ignore_changes = [ami]
  }
}

# ---------------------------------------------------------------------------
# Otomatik kapanma: CPU uzun süre düşükse sunucuyu durdur
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "idle_stop" {
  count = local.compute_count

  alarm_name        = "${var.project}-idle-stop"
  alarm_description = "CPU ${var.idle_minutes} dakika boyunca %${var.idle_cpu_threshold} altında kalırsa EC2'yi durdurur."

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
  treat_missing_data  = "missing" # sunucu kapalıyken veri gelmez; durumu değiştirme

  alarm_actions = ["arn:aws:automate:${var.region}:ec2:stop"]
}

# ---------------------------------------------------------------------------
# Çıktılar
# ---------------------------------------------------------------------------

output "bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "instance_id" {
  value = one(aws_instance.main[*].id)
}

output "ssm_connect" {
  description = "Sunucuya bağlanmak için (session-manager-plugin gerekir)."
  value       = var.enable_compute ? "aws ssm start-session --target ${aws_instance.main[0].id} --region ${var.region}" : null
}
