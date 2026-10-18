package main

import (
	"github.com/gorilla/mux"
	"net/http"
)


func main() {
	router := mux.NewRouter()

	router.HandleFunc("/users/track", handleTrack).Methods("POST")
	router.HandleFunc("/health", handleHealth).Methods("GET")

	http.ListenAndServe(":8081", router)
}

func handleTrack(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}
