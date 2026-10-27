package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu            sync.Mutex
	out           *os.File
	latency       int
	errorRate     int
	throttleUntil int
}

func (s *Server) appendLine(v any) error {
	jsonl, err := json.Marshal(v)
	if err != nil {
		return err
	}
	jsonl = append(jsonl, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.out.Write(jsonl)
	return err
}

type attribute struct {
	ExternalId string `json:"external_id"`
	Country    string `json:"country"`
	Language   string `json:"language"`
	Level      int    `json:"level"`
	ChurnRisk  string `json:"churn_risk"`
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
	ExternalId string  `json:"external_id"`
	ProductId  string  `json:"product_id"`
	Currency   string  `json:"currency"`
	Price      float64 `json:"price"`
	Quantity   int     `json:"quantity"`
	Time       string  `json:"time"`
}

type Body struct {
	Attributes []attribute `json:"attributes"`
	Events     []event     `json:"events"`
	Purchases  []purchase  `json:"purchases"`
}

func (b Body) Validate() error {
	for _, event := range b.Events {
		v := event.Validate()
		if v != nil {
			return v
		}
	}
	return nil
}

type Deletes struct {
	ExternalIds []string `json:"external_ids"`
}

type ErrorResponse struct {
	Message string   `json:"message"`
	Errors  []string `json:"errors"`
}

func (s *Server) handleTrack(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req Body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Error while parsing request body. Please check your syntax", err.Error())
		return
	}

	// Braze API indicates that the maximum amount of total items should not exceed 75 for some reason
	total_items := len(req.Attributes) + len(req.Events) + len(req.Purchases)
	if total_items > 75 {
		writeError(w, http.StatusBadRequest, "Max Input Length Exceeded", "Request contains more than 75 objects")
		return
	}

	if req.Validate() != nil {
		writeError(w, http.StatusBadRequest, "Max Input Length Exceeded", "Request contains more than 75 objects")
		return
	}

	payload, _ := io.ReadAll(r.Body)
	line := map[string]any{
		"received": time.Now().GoString(),
		"payload":  payload,
	}

	if err := s.appendLine(line); err != nil {
		writeError(w, http.StatusInternalServerError, "Unable to write file into memory. Try again ")
		return
	}

	success := map[string]any{
		"message":              "success",
		"attributes_processed": len(req.Attributes),
		"events_processed":     len(req.Events),
		"purchases_processed":  len(req.Purchases),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(success)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req Deletes
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "\"external_ids\" must be an array of strings", err.Error())
		return
	}
	if len(req.ExternalIds) > 50 {
		writeError(w, http.StatusBadRequest, "the max number of ids per request was exceeded")
		return
	}

	message := map[string]any{
		"deleted": len(req.ExternalIds),
		"message": "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(message)
}

func writeError(w http.ResponseWriter, status int, message string, errs ...string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Message: message,
		Errors:  errs,
	})
}
