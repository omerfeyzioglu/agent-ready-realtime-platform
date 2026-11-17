# ---------------------------------------------------------------------------
# Container images and the producer's AWS access.
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "producer" {
  count = var.enable_eks ? 1 : 0

  name                 = "${var.project}/producer"
  image_tag_mutability = "IMMUTABLE" # a tag always means the same image
  force_delete         = true        # terraform destroy removes stored images too

  image_scanning_configuration {
    scan_on_push = true # free basic scan for known vulnerabilities
  }
}

# Keep only the last 5 images; older ones are deleted (storage is billed).
resource "aws_ecr_lifecycle_policy" "producer" {
  count = var.enable_eks ? 1 : 0

  repository = aws_ecr_repository.producer[0].name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# The producer reads the frozen source files and nothing else.
data "aws_iam_policy_document" "producer_s3" {
  statement {
    sid       = "ListSource"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["source/tlc/fhvhv/*"]
    }
  }

  statement {
    sid       = "ReadSource"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/source/tlc/fhvhv/*"]
  }
}

resource "aws_iam_role" "producer" {
  count = var.enable_eks ? 1 : 0

  name               = "${local.eks_name}-producer"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

resource "aws_iam_role_policy" "producer_s3" {
  count = var.enable_eks ? 1 : 0

  name   = "read-source"
  role   = aws_iam_role.producer[0].id
  policy = data.aws_iam_policy_document.producer_s3.json
}

# Pods running as service account kafka/producer get this role.
resource "aws_eks_pod_identity_association" "producer" {
  count = var.enable_eks ? 1 : 0

  cluster_name    = module.eks.cluster_name
  namespace       = "kafka"
  service_account = "producer"
  role_arn        = aws_iam_role.producer[0].arn
}

output "producer_image_repo" {
  value = one(aws_ecr_repository.producer[*].repository_url)
}
