package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const maxBrazeObjects = 75

type BrazeSender struct {
	Client *http.Client
	URL    string
}

type BrazeEvent struct {
	ExternalId string         `json:"external_id"`
	Name       string         `json:"name"`
	Time       string         `json:"time"`
	Properties map[string]any `json:"properties"`
}

type BrazePurchase struct {
	ExternalId string         `json:"external_id"`
	ProductId  string         `json:"product_id"`
	Currency   string         `json:"currency"`
	Price      float64        `json:"price"`
	Quantity   int            `json:"quantity"`
	Time       string         `json:"time"`
	Properties map[string]any `json:"properties"`
}

type TrackRequest struct {
	Events    []BrazeEvent    `json:"events,omitempty"`
	Purchases []BrazePurchase `json:"purchases,omitempty"`
}

func (s BrazeSender) SendEvents(ctx context.Context, data []UserEvent) error {
	for i := 0; i < len(data); i += maxBrazeObjects {
		waitTime := time.Duration(250)
		end := min(i+maxBrazeObjects, len(data))
		// Production would add random jitter and poll events indefinetly so it wouldn't shut down
		for attempt := 0; attempt < 3; attempt++ {
			status, err := s.send(ctx, data[i:end])
			// 429 is awaited elsewhere
			if status >= 429 || err != nil {
				time.Sleep(time.Millisecond * waitTime)
				waitTime = waitTime * 2
				if attempt == 2 {
					recordFailed(data[i:end])
					return fmt.Errorf("Could not send data")
				}
				// Production would check other codes as well and behave differently. Currently Brazemock only sends this
			} else if status == 400 {
				recordFailed(data[i:end])
				return fmt.Errorf("Unable to send data to endpoint. Here's why %v\n", err)
			} else {
				recordSent(data[i:end])
				break
			}
			// this means that we tried 3 times without  succeeding
		}
	}
	return nil
}

func (s BrazeSender) send(ctx context.Context, data []UserEvent) (int, error) {
	inflightBatches.Inc()
	defer inflightBatches.Dec()

	var body TrackRequest

	for _, event := range data {
		props := map[string]any{"event_id": event.EventId, "produced_at": event.ProducedAt}
		t := time.UnixMilli(event.ProducedAt).UTC().Format(time.RFC3339)

		if event.Type == "purchase" {
			body.Purchases = append(body.Purchases, BrazePurchase{
				ExternalId: event.PlayerId,
				ProductId:  event.SKU,
				Currency:   event.Currency,
				Price:      event.Amount,
				Quantity:   1,
				Time:       t,
				Properties: props,
			})
		} else {
			body.Events = append(body.Events, BrazeEvent{
				ExternalId: event.PlayerId,
				Name:       event.Type,
				Time:       t,
				Properties: props,
			})
		}
	}

	payload, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Unable to create request body. Here's why %v\n", err)
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.URL+"/users/track", bytes.NewReader(payload))
	if err != nil {
		fmt.Printf("Unable to create request. Here's why %v\n", err)
		return 0, err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := s.Client.Do(req)
	if err != nil {
		recordBrazeRequest(0, err)
		fmt.Printf("Unable to send request. Here's why %v\n", err)
		return 0, err
	}
	defer res.Body.Close()
	recordBrazeRequest(res.StatusCode, nil)

	fmt.Printf("Sent %v events to braze with status code %v \n", len(body.Events)+len(body.Purchases), res.StatusCode)
	if res.StatusCode != 200 {
		text, _ := io.ReadAll(res.Body)
		fmt.Printf("Unable to send request. Here's why %v\n", string(text))
		headerValue := res.Header.Get("X-RateLimit-Reset")
		if headerValue != "" {
			sleepTime, _ := strconv.Atoi(headerValue)
			time.Sleep(time.Second * time.Duration(sleepTime))
		}
	}
	return res.StatusCode, nil
}
