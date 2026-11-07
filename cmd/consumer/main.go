package main

import (
	"io"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

type EventsStream struct {
	KinesisClient *kinesis.Client
	StreamName    string
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

	for iterator != nil {
		var events []UserEvent
		out, err := e.KinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
		})
		if err != nil {
			fmt.Printf("Could not Get Records. Here's why %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		for _, rec := range out.Records {
			var e UserEvent
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				fmt.Printf("Could not unmarshal event. Here's why %v\n", err)
				continue
			}
			fmt.Printf("%+v\n", e)
			events = append(events, e) 
		}
		if len(events) != 0 {
			sendBrazeEvents(ctx, events) 
		}
		iterator = out.NextShardIterator
		if len(out.Records) == 0 {
			time.Sleep(time.Millisecond * 250)
		}
	}
}

type event struct {
	ExternalId *string        `json:"external_id"`
	Name       *string        `json:"name"`
	Time       *string        `json:"time"`
	Properties map[string]any `json:"properties,omitempty"`
}

func (e event) Validate() error {
	if e.ExternalId == nil || e.Name == nil || e.Time == nil {
		return errors.New("Missing mandatory attribute. Please check syntax")
	}
	return nil
}

type purchase struct {
	ExternalId *string  `json:"external_id"`
	ProductId  *string  `json:"product_id"`
	Currency   *string  `json:"currency"`
	Price      *float64 `json:"price"`
	Quantity   int      `json:"quantity"`
	Time       *string  `json:"time"`
}

func sendBrazeEvents(ctx context.Context, data []UserEvent) int {

	var events []UserEvent
	var purchases []UserEvent

	for _, event := range data {
		props := map[string]any{"event_id" : event.EventId, "produced_at" : event.ProducedAt}
		if event.Type == "purchase" {
			purchases = append(purchases, event)
		} else {
			events = append(events, event)
		}
	}

	body := map[string][]UserEvent{
		"events":    events,
		"purchases": purchases,
	}
	payload, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://localhost:8081/users/track", bytes.NewReader(payload))
	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Unable to send request. Here's why %v\n", err)
		panic(err)
	}
	fmt.Printf("Sent %v events to braze with status code %v \n", len(events) + len(purchases), res.StatusCode) 
	if res.StatusCode != 200 {
		text, _ := io.ReadAll(res.Body)
		fmt.Printf("Unable to send request. Here's why %v\n", string(text))
	}
	return res.StatusCode
}

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	ct := CheckpointTable{
		TableName:      "checkpoints",
		DynamoDbClient: dynamodb.NewFromConfig(cfg),
	}
	k := EventsStream{
		StreamName:    "player-events",
		KinesisClient: kinesis.NewFromConfig(cfg),
	}
	input := k.getIterator(ctx, ct)
	k.readStream(ctx, input)

}
