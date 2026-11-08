package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type fakeBraze struct {
	mu       sync.Mutex
	statuses []int
	requests []TrackRequest
}

func (f *fakeBraze) handle(w http.ResponseWriter, r *http.Request) {
	var body TrackRequest
	json.NewDecoder(r.Body).Decode(&body)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, body)

	status := 200
	if len(f.statuses) > 0 {
		status = f.statuses[0]
		f.statuses = f.statuses[1:]
	}
	w.WriteHeader(status)
}

func newSender(t *testing.T, statuses ...int) (BrazeSender, *fakeBraze) {
	f := &fakeBraze{statuses: statuses}
	server := httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(server.Close)
	return BrazeSender{Client: server.Client(), URL: server.URL}, f
}

func levelUps(n int) []UserEvent {
	list := make([]UserEvent, n)
	for i := range list {
		list[i] = UserEvent{
			EventId:    fmt.Sprintf("e-%d", i),
			PlayerId:   "p-1",
			Type:       "level_up",
			ProducedAt: 1762509600000,
		}
	}
	return list
}

func TestSendEventsSplitsIntoChunksOf75(t *testing.T) {
	s, f := newSender(t)

	if err := s.SendEvents(context.Background(), levelUps(160)); err != nil {
		t.Fatal(err)
	}

	if len(f.requests) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(f.requests))
	}
	for i, want := range []int{75, 75, 10} {
		if got := len(f.requests[i].Events); got != want {
			t.Fatalf("request %d: expected %d events, got %d", i, want, got)
		}
	}
}

func TestSendEventsMapsEventsAndPurchases(t *testing.T) {
	s, f := newSender(t)
	data := []UserEvent{
		{EventId: "e-1", PlayerId: "p-1", Type: "level_up", ProducedAt: 1762509600000},
		{EventId: "e-2", PlayerId: "p-2", Type: "purchase", ProducedAt: 1762509600000, Amount: 5.99, Currency: "EUR", SKU: "gem_pack"},
	}

	if err := s.SendEvents(context.Background(), data); err != nil {
		t.Fatal(err)
	}

	body := f.requests[0]
	if len(body.Events) != 1 || len(body.Purchases) != 1 {
		t.Fatalf("expected 1 event and 1 purchase, got %d and %d", len(body.Events), len(body.Purchases))
	}

	event := body.Events[0]
	if event.ExternalId != "p-1" || event.Name != "level_up" || event.Time != "2025-11-07T10:00:00Z" {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.Properties["event_id"] != "e-1" || event.Properties["produced_at"] != float64(1762509600000) {
		t.Fatalf("unexpected event properties: %v", event.Properties)
	}

	purchase := body.Purchases[0]
	if purchase.ExternalId != "p-2" || purchase.ProductId != "gem_pack" || purchase.Price != 5.99 || purchase.Currency != "EUR" || purchase.Quantity != 1 {
		t.Fatalf("unexpected purchase: %+v", purchase)
	}
	if purchase.Properties["event_id"] != "e-2" {
		t.Fatalf("unexpected purchase properties: %v", purchase.Properties)
	}
}

func TestSendEventsRetriesOnServerError(t *testing.T) {
	s, f := newSender(t, 500, 200)

	if err := s.SendEvents(context.Background(), levelUps(1)); err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}

	if len(f.requests) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(f.requests))
	}
}

func TestSendEventsRetriesOnRateLimit(t *testing.T) {
	s, f := newSender(t, 429, 200)

	if err := s.SendEvents(context.Background(), levelUps(1)); err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}

	if len(f.requests) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(f.requests))
	}
}

func TestSendEventsGivesUpAfterThreeAttempts(t *testing.T) {
	s, f := newSender(t, 500, 500, 500)

	if err := s.SendEvents(context.Background(), levelUps(1)); err == nil {
		t.Fatal("expected an error")
	}

	if len(f.requests) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(f.requests))
	}
}

func TestSendEventsDoesNotRetryBadRequest(t *testing.T) {
	s, f := newSender(t, 400)

	if err := s.SendEvents(context.Background(), levelUps(1)); err == nil {
		t.Fatal("expected an error")
	}

	if len(f.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(f.requests))
	}
}
