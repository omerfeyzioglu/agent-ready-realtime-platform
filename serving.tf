# ---------------------------------------------------------------------------
# AWS access for the ops API (service account serving/api). Read-only, and
# only the tables its tools serve: the hourly features and their scores,
# the live minute log (Iceberg), zones and holidays. No raw, no source, no
# landing, no writes anywhere.
# ---------------------------------------------------------------------------

locals {
  api_read_prefixes = [
    "lake/features/zone_hour/",
    "lake/features/zone_hour_vs_baseline/",
    "lake/features/zone_minute_live/",
    "lake/staging/zones/",
    "lake/staging/holidays/",
  ]
}

data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ListServedTables"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [for p in local.api_read_prefixes : "${p}*"]
    }
  }

  statement {
    sid       = "ReadServedTables"
    actions   = ["s3:GetObject"]
    resources = [for p in local.api_read_prefixes : "${aws_s3_bucket.raw.arn}/${p}*"]
  }

  # The minute log is Iceberg: its current metadata file is found in Glue.
  statement {
    sid     = "GlueZoneMinuteLive"
    actions = ["glue:GetTable"]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/features",
      "${local.glue_arn}:table/features/feat_zone_minute_live",
    ]
  }
}

resource "aws_iam_role" "api" {
  count = var.enable_eks ? 1 : 0

  name               = "${local.eks_name}-api"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy" "api" {
  count = var.enable_eks ? 1 : 0

  name   = "read-served-tables"
  role   = aws_iam_role.api[0].id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_eks_pod_identity_association" "api" {
  count = var.enable_eks ? 1 : 0

  cluster_name    = module.eks.cluster_name
  namespace       = "serving"
  service_account = "api"
  role_arn        = aws_iam_role.api[0].arn
}
