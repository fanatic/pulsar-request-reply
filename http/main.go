package main

import (
	"fmt"
	"net/http"
)

func main() {
	go welcomeServer()
	bridge()

	// curl -v http://localhost:8000/welcome-service
}

func welcomeServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Welcome to my website!")
	})

	http.ListenAndServe(":8090", nil)
}
