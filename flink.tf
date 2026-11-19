# ---------------------------------------------------------------------------
# AWS access for Flink jobs (service account flink/flink, created by the
# Flink operator's Helm chart). The raw writer appends to raw.trip_events
# and keeps checkpoints and HA metadata under ops/flink/.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "flink" {
  statement {
    sid       = "ListOwnPrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["lake/raw/trip_events/*", "ops/flink/*"]
    }
  }

  statement {
    sid     = "ReadWriteOwnPrefixes"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.raw.arn}/lake/raw/trip_events/*",
      "${aws_s3_bucket.raw.arn}/ops/flink/*",
    ]
  }

  # Iceberg commits by swapping the table's metadata pointer in Glue.
  statement {
    sid     = "GlueRawTripEvents"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/raw",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/raw/trip_events",
    ]
  }
}

resource "aws_iam_role" "flink" {
  count = var.enable_eks ? 1 : 0

  name               = "${local.eks_name}-flink"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy" "flink" {
  count = var.enable_eks ? 1 : 0

  name   = "raw-writer"
  role   = aws_iam_role.flink[0].id
  policy = data.aws_iam_policy_document.flink.json
}

resource "aws_eks_pod_identity_association" "flink" {
  count = var.enable_eks ? 1 : 0

  cluster_name    = module.eks.cluster_name
  namespace       = "flink"
  service_account = "flink"
  role_arn        = aws_iam_role.flink[0].arn
}

# ---------------------------------------------------------------------------
# live-features job: besides Redis it appends one row per zone, provider and
# minute to features.feat_zone_minute_live (history, skew checks). The job
# creates the table on first start, hence CreateTable on that one name.
# ---------------------------------------------------------------------------

locals {
  glue_arn = "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}"
}

data "aws_iam_policy_document" "flink_live_features" {
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["lake/features/zone_minute_live/*"]
    }
  }

  statement {
    sid       = "ReadWriteOwnPrefix"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.raw.arn}/lake/features/zone_minute_live/*"]
  }

  statement {
    sid     = "GlueZoneMinuteLive"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/features",
      "${local.glue_arn}:table/features/feat_zone_minute_live",
    ]
  }
}

resource "aws_iam_role_policy" "flink_live_features" {
  count = var.enable_eks ? 1 : 0

  name   = "live-features"
  role   = aws_iam_role.flink[0].id
  policy = data.aws_iam_policy_document.flink_live_features.json
}
