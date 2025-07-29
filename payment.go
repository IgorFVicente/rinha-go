package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func paymentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)

	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("Received payment data: %s", string(body))

	var jsonData map[string]interface{}

	if err := json.Unmarshal(body, &jsonData); err != nil {
		log.Printf("Warning: received non-JSON data or malformed JSON: %v", err)
	} else {
		prettyJson, _ := json.MarshalIndent(jsonData, "", " ")
		log.Printf("Pretty JSON:\n%s", string(prettyJson))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": "received", "message": "Payment data logged successfully"}`)
}

func main() {
	http.HandleFunc("/payments", paymentsHandler)

	fmt.Println("Server starting on port 9999")
	log.Fatal(http.ListenAndServe(":9999", nil))
}
