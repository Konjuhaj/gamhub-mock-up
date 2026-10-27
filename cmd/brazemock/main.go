package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"strconv"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getcontrolenv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		cv, err := strconv.Atoi(key)
		if  err != nil {
			return cv
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
		out: f,
		throttleUntil : getcontrolenv("THROTTLE_UNTIL", 0),
		latency : getcontrolenv("LATENCY", 0),
		errorRate : getcontrolenv("ERROR_RATE", 0),
	}
	router := mux.NewRouter()

	router.HandleFunc("/users/track", s.handleTrack).Methods("POST")
	router.HandleFunc("/users/delete", s.handleDelete).Methods("POST")
	router.HandleFunc("/health", s.handleHealth).Methods("GET")

	http.ListenAndServe(":8081", router)
}
