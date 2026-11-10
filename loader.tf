# ---------------------------------------------------------------------------
# Geçici loader: kaynak dosyaları us-east-1 içinden indirip landing/'e yükler.
# İş bitince enable_loader = false ile silinir. Gelen port yok; komutlar SSM
# Run Command ile gönderilir (session-manager-plugin gerekmez).
# ---------------------------------------------------------------------------

variable "enable_loader" {
  description = "Geçici loader EC2'sini ve ona bağlı kaynakları oluştur."
  type        = bool
  default     = false
}

variable "loader_instance_type" {
  description = "Loader sunucusunun tipi (ağ ağırlıklı iş, küçük yeter)."
  type        = string
  default     = "c7i.large"
}

variable "loader_max_hours" {
  description = "Unutulursa loader bu kadar saat sonra kendini kapatıp silinir."
  type        = number
  default     = 3
}

locals {
  loader_count = var.enable_loader ? 1 : 0
}

resource "aws_s3_object" "loader_script" {
  count = local.loader_count

  bucket      = aws_s3_bucket.raw.id
  key         = "ops/code/load_landing.py"
  source      = "${path.module}/ingest/load_landing.py"
  source_hash = filesha256("${path.module}/ingest/load_landing.py")
}

data "aws_iam_policy_document" "loader_bucket" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]
  }

  statement {
    sid     = "ReadWriteLandingAndOps"
    actions = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.raw.arn}/landing/*",
      "${aws_s3_bucket.raw.arn}/ops/*",
      "${aws_s3_bucket.raw.arn}/source/*",
      "${aws_s3_bucket.raw.arn}/lake/raw/*",
      "${aws_s3_bucket.raw.arn}/lake/staging/*",
    ]
  }

  statement {
    sid = "GlueRawTables"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.raw.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw.name}/*",
    ]
  }
}

resource "aws_iam_role" "loader" {
  count = local.loader_count

  name               = "${var.project}-loader"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "loader_ssm" {
  count = local.loader_count

  role       = aws_iam_role.loader[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "loader_bucket" {
  count = local.loader_count

  name   = "landing-ops-write"
  role   = aws_iam_role.loader[0].id
  policy = data.aws_iam_policy_document.loader_bucket.json
}

resource "aws_iam_instance_profile" "loader" {
  count = local.loader_count

  name = "${var.project}-loader"
  role = aws_iam_role.loader[0].name
}

resource "aws_security_group" "loader" {
  count = local.loader_count

  name        = "${var.project}-loader"
  description = "Gelen port yok; erisim SSM ile. Giden trafik acik (indirme, S3, SSM)."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Tum giden trafik"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "loader" {
  count = local.loader_count

  ami                                  = data.aws_ami.al2023.id
  instance_type                        = var.loader_instance_type
  subnet_id                            = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids               = [aws_security_group.loader[0].id]
  iam_instance_profile                 = aws_iam_instance_profile.loader[0].name
  associate_public_ip_address          = true
  instance_initiated_shutdown_behavior = "terminate"

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  # Emniyet: unutulursa kendini kapatır (shutdown davranışı "terminate").
  user_data = <<-EOF
    #!/bin/bash
    shutdown -h +${var.loader_max_hours * 60}
  EOF

  tags = {
    Name = "${var.project}-loader"
  }

  depends_on = [aws_s3_object.loader_script]
}

output "loader_instance_id" {
  value = one(aws_instance.loader[*].id)
}
