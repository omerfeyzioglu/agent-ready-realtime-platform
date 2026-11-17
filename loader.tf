# ---------------------------------------------------------------------------
# Temporary loader: runs batch jobs (downloads, builds) inside us-east-1, next
# to the data. Removed with enable_loader = false when the job is done. No
# inbound ports; commands are sent with SSM Run Command (no session-manager-plugin).
# ---------------------------------------------------------------------------

variable "enable_loader" {
  description = "Create the temporary loader EC2 instance and its resources."
  type        = bool
  default     = false
}

variable "loader_instance_type" {
  description = "Loader instance type (small is enough for downloads; larger for heavy builds)."
  type        = string
  default     = "c7i.large"
}

variable "loader_max_hours" {
  description = "If forgotten, the loader shuts down and terminates itself after this many hours."
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
      "${aws_s3_bucket.raw.arn}/lake/features/*",
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
  description = "No inbound ports; access via SSM. Outbound open (downloads, S3, SSM)."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All outbound traffic"
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

  # Safety net: shuts itself down if forgotten (shutdown behavior is "terminate").
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
