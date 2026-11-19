# ---------------------------------------------------------------------------
# Iceberg catalog: AWS Glue Data Catalog. Tables live under lake/ in the
# bucket; Glue only stores their metadata pointers.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "raw" {
  name         = "raw"
  description  = "Raw layer: append-only Iceberg tables (events as received)"
  location_uri = "s3://${aws_s3_bucket.raw.id}/lake/raw"
}

output "glue_raw_database" {
  value = aws_glue_catalog_database.raw.name
}

resource "aws_glue_catalog_database" "features" {
  name         = "features"
  description  = "Feature layer: Iceberg tables written by streaming jobs (batch features stay plain Parquet under lake/features)"
  location_uri = "s3://${aws_s3_bucket.raw.id}/lake/features"
}
