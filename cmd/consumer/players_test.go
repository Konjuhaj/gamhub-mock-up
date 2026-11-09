package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type fakePlayers map[string]Player

func (f fakePlayers) getPlayer(ctx context.Context, id string) (Player, bool, error) {
	p, ok := f[id]
	return p, ok, nil
}

type brokenPlayers struct{}

func (brokenPlayers) getPlayer(ctx context.Context, id string) (Player, bool, error) {
	return Player{}, false, errors.New("dynamo down")
}

func records(t *testing.T, playerIds ...string) []ktypes.Record {
	var list []ktypes.Record
	for i, id := range playerIds {
		data, err := json.Marshal(UserEvent{EventId: id + "-event", PlayerId: id, Type: "level_up", ProducedAt: 1762509600000 + int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		list = append(list, ktypes.Record{Data: data})
	}
	return list
}

func TestSuppressReason(t *testing.T) {
	tests := []struct {
		name   string
		player Player
		found  bool
		want   string
	}{
		{"adult", Player{AgeKnown: true}, true, ""},
		{"minor", Player{AgeKnown: true, IsMinor: true}, true, "minor"},
		{"age unknown", Player{AgeKnown: false}, true, "age_unknown"},
		{"deleted", Player{AgeKnown: true, Deleted: true}, true, "deleted"},
		{"deleted minor", Player{AgeKnown: true, IsMinor: true, Deleted: true}, true, "deleted"},
		{"not found", Player{}, false, "age_unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := suppressReason(tt.player, tt.found); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestReplayOnlyAdultReachesBraze(t *testing.T) {
	s, f := newSender(t)
	e := EventsStream{
		Sender: s,
		Pt: fakePlayers{
			"p-adult":   {Id: "p-adult", AgeKnown: true},
			"p-minor":   {Id: "p-minor", AgeKnown: true, IsMinor: true},
			"p-unknown": {Id: "p-unknown", AgeKnown: false},
			"p-deleted": {Id: "p-deleted", AgeKnown: true, Deleted: true},
		},
	}

	events, err := e.filterRecords(context.Background(), records(t, "p-adult", "p-minor", "p-unknown", "p-deleted", "p-missing"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Sender.SendEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}

	if len(f.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(f.requests))
	}
	sent := f.requests[0].Events
	if len(sent) != 1 || sent[0].ExternalId != "p-adult" {
		t.Fatalf("expected only p-adult, got %+v", sent)
	}
}

func TestReplayAllSuppressedSendsNothing(t *testing.T) {
	s, f := newSender(t)
	e := EventsStream{
		Sender: s,
		Pt:     fakePlayers{"p-minor": {Id: "p-minor", AgeKnown: true, IsMinor: true}},
	}

	events, err := e.filterRecords(context.Background(), records(t, "p-minor"))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Sender.SendEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}

	if len(f.requests) != 0 {
		t.Fatalf("expected no requests, got %d", len(f.requests))
	}
}

func TestFilterRecordsStopsOnLookupError(t *testing.T) {
	e := EventsStream{Pt: brokenPlayers{}}

	if _, err := e.filterRecords(context.Background(), records(t, "p-adult")); err == nil {
		t.Fatal("expected an error")
	}
}
