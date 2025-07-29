package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type PaymentData struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type ForwardedPaymentData struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

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

	var paymentData PaymentData
	if err := json.Unmarshal(body, &paymentData); err != nil {
		log.Printf("Error parsing JSON: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	forwardedData := ForwardedPaymentData{
		CorrelationId: paymentData.CorrelationId,
		Amount:        paymentData.Amount,
		RequestedAt:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	jsonData, err := json.Marshal(forwardedData)

	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	resp, err := http.Post("http://localhost:8001/payments", "application/json", bytes.NewBuffer(jsonData))
	log.Printf("Teste: %v", resp)
	if err != nil {
		log.Printf("Error sending request to localhost:8001: %v", err)
		http.Error(w, "Error forwarding request", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	log.Printf("Forwarded to localhost:8001, response status: %d", resp.StatusCode)
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/payments", paymentsHandler)

	fmt.Println("Server starting on port 9999")
	log.Fatal(http.ListenAndServe(":9999", nil))
}
