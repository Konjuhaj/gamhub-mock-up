data "aws_caller_identity" "current" {}

resource "aws_iam_role" "player-events-consumer" {
  name = "player-events-consumer"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
      }
    ]
  })
}

data "aws_iam_policy_document" "events-stream-policy" {
  statement {
    sid       = "KinesisRead"
    actions   = ["kinesis:ListShards", "kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords"]
    resources = [resource.aws_kinesis_stream.player-events.arn]
  }

  statement {
    sid       = "DynamoReadWrite"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.player-state-table.arn, aws_dynamodb_table.checkpoint-table.arn]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "player-events-consumer-policy"
  role   = aws_iam_role.player-events-consumer.id
  policy = data.aws_iam_policy_document.events-stream-policy.json
}

output "consumer_role_arn" {
  value       = aws_iam_role.player-events-consumer.arn
  description = "arn for consuming data"
}
