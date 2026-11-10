package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Player struct {
	Id       string `dynamodbav:"player_id"`
	IsMinor  bool   `dynamodbav:"is_minor"`
	AgeKnown bool   `dynamodbav:"age_known"`
	Deleted  bool   `dynamodbav:"deleted"`
}

type PlayerLookup interface {
	getPlayer(ctx context.Context, id string) (Player, bool, error)
}

type PlayersTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (pt PlayersTable) getPlayer(ctx context.Context, id string) (Player, bool, error) {
	var p Player

	out, err := pt.DynamoDbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(pt.TableName),
		Key:       map[string]dtypes.AttributeValue{"player_id": &dtypes.AttributeValueMemberS{Value: id}},
	})
	if err != nil || len(out.Item) == 0 {
		fmt.Printf("Unable to find player. Here's why %v\n", err)
		return p, false, err
	}

	err = attributevalue.UnmarshalMap(out.Item, &p)
	if err != nil {
		fmt.Printf("Unable to find player. Here's why %v\n", err)
		return p, false, err
	}
	return p, true, err
}

func suppressReason(p Player, found bool) string {
	switch {
	case p.Deleted:
		return "deleted"
	case !p.AgeKnown || !found:
		return "age_unknown"
	case p.IsMinor:
		return "minor"
	}

	return ""
}

func markFirstPurchase(ctx, playerId, eventId string, at int64) (bool, error) {

}
