resource "aws_dynamodb_table" "player-state-table" {
    name            =   "player_state"
    billing_mode    =   "PROVISIONED"
    hash_key        =   "player_id"

    attribute { 
        name = "player_id"
        type = "S"
    }
    
}

resource "aws_dynamodb_table" "checkpoint-table" {
    name            =   "checkpoint"
    billing_mode    =   "PROVISIONED"
    hash_key        =   "checkpoint_id"

    attribute { 
        name = "checkpoint_id"
        type = "S"
    }
    
}
