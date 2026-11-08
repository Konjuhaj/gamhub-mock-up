package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type UserEvent struct {
	EventId    string  `json:"event_id"`
	PlayerId   string  `json:"player_id"`
	Type       string  `json:"type"`
	ProducedAt int64   `json:"produced_at"`
	Amount     float64 `json:"amount,omitempty"`
	Currency   string  `json:"currency,omitempty"`
	SKU        string  `json:"sku,omitempty"`
}

type CheckpointTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}
type Checkpoint struct {
	ShardId        string `dynamodbav:"shard_id"`
	SequenceNumber string `dynamodbav:"sequence_number"`
	UpdatedAt      int64  `dynamodbav:"updated_at"`
}

func (t CheckpointTable) getLastCheckpoint(ctx context.Context) (Checkpoint, error) {
	var checkpoints Checkpoint

	res, err := t.DynamoDbClient.GetItem(ctx, &dynamodb.GetItemInput{
		Key:       map[string]dtypes.AttributeValue{"shard_id": &dtypes.AttributeValueMemberS{Value: "shardId-000000000000"}},
		TableName: aws.String(t.TableName),
	})
	if err != nil {
		fmt.Printf("Could not find latest checkpoints. Here's why %v\n", err)
		return checkpoints, err
	}
	err = attributevalue.UnmarshalMap(res.Item, &checkpoints)
	if err != nil {
		fmt.Printf("Could not find latest checkpoints. Here's why %v\n", err)
		return checkpoints, err
	}
	return checkpoints, nil
}

func (t CheckpointTable) saveCheckpoint(ctx context.Context, seq string) {
	// In a real world production setting we would check weather we're able to save the checkpoint.
	// since this is a controlled PoC it's not necessary
	item, err := attributevalue.MarshalMap(Checkpoint{
		ShardId:        "shardId-000000000000",
		SequenceNumber: seq,
		UpdatedAt:      time.Now().UnixMilli(),
	})
	if err != nil {
		fmt.Printf("Could not marshal map sequence number. Here's why %v\n", err)
	}
	_, err = t.DynamoDbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(t.TableName),
		Item:      item,
	})
	if err != nil {
		fmt.Printf("Could not save checkpoint. Here's why %v\n", err)
	}
}

type EventsStream struct {
	KinesisClient *kinesis.Client
	StreamName    string
	Sender        BrazeSender
	Ct            CheckpointTable
}

func (e *EventsStream) getIterator(ctx context.Context, ct CheckpointTable) *kinesis.GetShardIteratorOutput {
	input := &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(e.StreamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: ktypes.ShardIteratorTypeTrimHorizon,
	}
	cp, err := ct.getLastCheckpoint(ctx)
	if err != nil {
		fmt.Printf("Could not read stream. Here's why %v\n", err)
		panic(err)
	}
	if cp.SequenceNumber != "" {
		input.ShardIteratorType = ktypes.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(cp.SequenceNumber)
	}
	it, err := e.KinesisClient.GetShardIterator(ctx, input)
	if err != nil {
		fmt.Printf("Could not get Shard Iterator. Here's why %v\n", err)
		panic(err)
	}
	return it
}

func (e *EventsStream) readStream(ctx context.Context, it *kinesis.GetShardIteratorOutput) {
	iterator := it.ShardIterator
	// the send and checkpoint always finish, even after Ctrl-C, so a batch is never left half done
	work := context.WithoutCancel(ctx)

	for iterator != nil {
		if ctx.Err() != nil {
			fmt.Println("Shutting down, last checkpoint saved")
			return
		}
		var events []UserEvent
		out, err := e.KinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
		})
		if err != nil {
			fmt.Printf("Could not Get Records. Here's why %v\n", err)
			time.Sleep(time.Second)
			continue
		}
		if out.MillisBehindLatest != nil {
			consumerLag.Set(float64(*out.MillisBehindLatest))
		}

		for _, rec := range out.Records {
			var e UserEvent
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				fmt.Printf("Could not unmarshal event. Here's why %v\n", err)
				continue
			}
			events = append(events, e)
		}
		if len(out.Records) != 0 {
			err := e.Sender.SendEvents(work, events)
			if err != nil {
				panic(err)
			}
			e.Ct.saveCheckpoint(work, *out.Records[len(out.Records)-1].SequenceNumber)
		}

		iterator = out.NextShardIterator
		time.Sleep(time.Millisecond * 250)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	ct := CheckpointTable{
		TableName: "checkpoints",
		DynamoDbClient: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.DisableValidateResponseChecksum = true
		}),
	}
	sender := BrazeSender{
		Client: &http.Client{Timeout: 10 * time.Second},
		URL:    "http://localhost:8081",
	}
	k := EventsStream{
		StreamName:    "player-events",
		KinesisClient: kinesis.NewFromConfig(cfg),
		Sender:        sender,
		Ct:            ct,
	}
	go startMetricsServer(":2112")
	input := k.getIterator(ctx, ct)
	k.readStream(ctx, input)

}
