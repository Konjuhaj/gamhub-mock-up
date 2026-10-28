package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getenvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
func main() {
	path := "/data/received.jsonl"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	s := &Server{
		out:           f,
		Latency:       time.Duration(getenvInt("LATENCY_MS", 0)) * time.Millisecond,
		ErrorRate:     getenvFloat("ERROR_RATE", 0),
		ThrottleUntil: time.Now().Add(time.Duration(getenvInt("THROTTLE_SECONDS", 0)) * time.Second),
	}
	router := mux.NewRouter()

	router.HandleFunc("/users/track", s.handleTrack).Methods("POST")
	router.HandleFunc("/users/delete", s.handleDelete).Methods("POST")
	router.HandleFunc("/admin/failure", s.setFailure).Methods("POST")
	router.HandleFunc("/health", s.handleHealth).Methods("GET")

	http.ListenAndServe(":8081", router)
}
