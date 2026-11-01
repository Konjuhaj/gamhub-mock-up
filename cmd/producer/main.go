package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"math/rand/v2"
)

type Player struct {
	Id       string `json:"id"`
	IsMinor  bool   `json:"is_minor"`
	AgeKnown bool   `json:"age_known"`
	Deleted  bool   `json:"deleted"`
}

var Players []Player

type UserEvent struct {
	EventId  string `json:"event_id"`
	PlayerId string `json:"player_id"`
	Type     string `json:"type"`
	Produced string `json:"produced"`

	// only for purchases
	Amount   float64 `json:"amount,omitempty"`
	Currency string  `json:"currency,omitempty"`
	SKU      string  `json:"sku,omitempty"`
}

func createPlayers() {
	for i := range 250 {

		var p = Player{
			Id:       fmt.Sprintf("p-%d", i+1),
			IsMinor:  false,
			AgeKnown: true,
			Deleted:  false,
		}

		seed := rand.Float32()
		switch {
		case seed <= 0.15:
			p.IsMinor = true
		case seed <= 0.10:
			p.AgeKnown = false
		case seed <= 0.05:
			p.Deleted = true
		}

		Players = append(Players, p)
	}
}
