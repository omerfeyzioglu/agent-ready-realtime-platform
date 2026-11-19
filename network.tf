# ---------------------------------------------------------------------------
# Network: the default VPC and its default subnets (no NAT, no load balancers)
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
