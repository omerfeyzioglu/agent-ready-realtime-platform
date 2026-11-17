# ---------------------------------------------------------------------------
# Budget alarm: an email when the month's spend passes a limit. Free (the
# first two budgets of an account cost nothing).
#
# Credits are excluded from the calculation: this account pays with
# credits, and with credits included the "actual cost" stays at 0 and the
# alarm would never fire.
#
# The address is not in the code (the repo goes public); set it with
# TF_VAR_budget_alert_email or a .tfvars file (both stay out of git).
# ---------------------------------------------------------------------------

variable "budget_alert_email" {
  description = "Email that receives budget alerts. Empty: no budget is created."
  type        = string
  default     = ""
}

variable "budget_monthly_usd" {
  description = "Monthly spend limit in USD."
  type        = number
  default     = 40
}

resource "aws_budgets_budget" "monthly" {
  count = var.budget_alert_email == "" ? 0 : 1

  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = var.budget_monthly_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_types {
    include_credit = false
    include_refund = false
  }

  # Half the limit: an early warning.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  # The full limit has been spent.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  # The month is on track to pass the limit.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
