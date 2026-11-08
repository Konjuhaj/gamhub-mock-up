package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"math/rand/v2"
)

type PlayersTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

type EventsStream struct {
	KinesisClient *kinesis.Client
	StreamName    string
}

type Player struct {
	Id       string `dynamodbav:"player_id"`
	IsMinor  bool   `dynamodbav:"is_minor"`
	AgeKnown bool   `dynamodbav:"age_known"`
	Deleted  bool   `dynamodbav:"deleted"`
}

var Players []Player

type UserEvent struct {
	EventId    string `json:"event_id"`
	PlayerId   string `json:"player_id"`
	Type       string `json:"type"`
	ProducedAt int64  `json:"produced_at"`

	// only for purchases
	Amount   float64 `json:"amount,omitempty"`
	Currency string  `json:"currency,omitempty"`
	SKU      string  `json:"sku,omitempty"`
}

func main() {

	rate := flag.Int("rate", 10, "events per second")
	burst := flag.Bool("burst", false, "send at 10x the rate")
	count := flag.Int("count", 0, "number of events to send, 0 means forever")
	seedOnly := flag.Bool("seed-only", false, "write the players to DynamoDB and exit")
	flag.Parse()

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	pt := PlayersTable{
		TableName:      "player_state",
		DynamoDbClient: dynamodb.NewFromConfig(cfg),
	}

	pt.AddPlayers(ctx)
	if *seedOnly {
		pt.SeedPlayers(ctx)
		return
	}

	et := EventsStream{
		StreamName:    "player-events",
		KinesisClient: kinesis.NewFromConfig(cfg),
	}
	et.CreateEvents(ctx, *rate, *count, *burst)

}

var r = rand.New(rand.NewPCG(42, 42))

func (t PlayersTable) AddPlayers(ctx context.Context) {

	for i := range 250 {
		p := Player{
			Id:       fmt.Sprintf("p-%d", i+1),
			IsMinor:  false,
			AgeKnown: true,
			Deleted:  false,
		}

		seed := r.Float32()
		switch {
		case seed <= 0.05:
			p.Deleted = true
		case seed <= 0.10:
			p.AgeKnown = false
		case seed <= 0.15:
			p.IsMinor = true
		}
		Players = append(Players, p)

	}
}

func (t PlayersTable) SeedPlayers(ctx context.Context) {
	for _, p := range Players {
		item, err := attributevalue.MarshalMap(p)
		if err != nil {
			panic(err)
		}
		_, err = t.DynamoDbClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(t.TableName), Item: item,
		})

		if err != nil {
			fmt.Printf("Couldn't add item to table. Here's why: %v\n", err)
		}
	}
}

func (s EventsStream) CreateEvents(ctx context.Context, rate, count int, burst bool) {
	perSecond := rate
	if burst {
		perSecond *= 10
	}

	ticker := time.NewTicker(time.Second / time.Duration(perSecond))
	defer ticker.Stop()

	sent := 0
	for count == 0 || sent < count {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			produceEvent(s, ctx)
			sent++
		}
	}
}

type Product struct {
	SKU   string
	Price float64
}

func produceEvent(s EventsStream, ctx context.Context) {
	p := Players[r.IntN(250)]
	e := []string{"purchase", "level_up", "session_start"}[r.IntN(3)]

	var catalog = []Product{
		{"gem_pack_small", 0.99},
		{"gem_pack", 5.99},
		{"gem_pack_large", 10.99},
		{"season_pass", 19.99},
	}

	event := UserEvent{
		EventId:    fmt.Sprintf("e-%d-%d", time.Now().UnixNano(), r.Int()),
		PlayerId:   p.Id,
		Type:       e,
		ProducedAt: time.Now().UnixMilli(),
	}

	if e == "purchase" {
		product := catalog[r.IntN(len(catalog))]
		event.SKU = product.SKU
		event.Amount = product.Price
		event.Currency = "EUR"
	}

	data, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("Unable to create event. Here's why %v\n", err)
		return
	}

	_, err = s.KinesisClient.PutRecord(ctx, &kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(s.StreamName),
		PartitionKey: aws.String(event.PlayerId),
	})

	if err != nil {
		fmt.Printf("Could not add event to stream. Here's why %v\n", err)
		return
	}
}
