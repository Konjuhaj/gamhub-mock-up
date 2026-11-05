package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type CheckpointTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}
type Checkpoint struct {
	ShardId        string `json:"shard_id"`
	SequenceNumber string `json:"sequence_number"`
}

func (t CheckpointTable) getLastCheckpoint(ctx context.Context) (Checkpoint, error) {
	var checkpoint Checkpoint

	res, err := t.DynamoDbClient.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]dtypes.AttributeValue{"shard_id": &dtypes.AttributeValueMemberS{Value: "shardId-000000000000"}},
		TableName : aws.String(t.TableName),
	})
	if err != nil {
		fmt.Printf("Could not find latest checkpoint. Here's why %v\n", err)
		return checkpoint, err
	}
	err = attributevalue.UnmarshalMap(res.Item, &checkpoint)
	if err != nil {
		fmt.Printf("Could not find latest checkpoint. Here's why %v\n", err)
		return checkpoint, err
	}
	return checkpoint, nil
}

type EventsStream struct {
	KinesisClient *kinesis.Client
	StreamName    string
}

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	ct := CheckpointTable{
		TableName:      "checkpoint",
		DynamoDbClient: dynamodb.NewFromConfig(cfg),
	}

	k := EventsStream{
		StreamName:    "player-events",
		KinesisClient: kinesis.NewFromConfig(cfg),
	}

	cp, err := ct.GetLastCheckpint(ctx)
	it, err := k.KinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(k.StreamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: ktypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		fmt.Printf("Could not read stream. Here's why %v\n", err) 
	}
	

}
