resource "aws_dynamodb_table" "player-state-table" {
  name         = "player_state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "player_id"


  attribute {
    name = "player_id"
    type = "S"
  }

}

resource "aws_dynamodb_table" "checkpoint-table" {
  name         = "checkpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "shard_id"

  attribute {
    name = "shard_id"
    type = "S"
  }

}
