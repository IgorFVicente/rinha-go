package handlers

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/models"
	"payment-service/internal/services"
)

type PaymentHandler struct {
	paymentService *services.PaymentService
	queueService   *services.QueueService
	cfg            *config.Config
}

func NewPaymentHandler(paymentService *services.PaymentService, queueService *services.QueueService) *PaymentHandler {
	return &PaymentHandler{
		paymentService: paymentService,
		queueService:   queueService,
	}
}

func (h *PaymentHandler) HandlePayments(w http.ResponseWriter, r *http.Request) {
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

	var paymentData models.PaymentData
	if err := json.Unmarshal(body, &paymentData); err != nil {
		log.Printf("Error parsing JSON: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	job := models.PaymentJob{
		CorrelationId: paymentData.CorrelationId,
		Amount:        paymentData.Amount,
		RequestedAt:   time.Now().UTC().Truncate(time.Millisecond),
		RetryCount:    0,
		MaxRetries:    3, // This could come from config
	}

	if err := h.queueService.EnqueueJob(job); err != nil {
		log.Printf("Error enqueuing payment job: %v", err)
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *PaymentHandler) HandlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var fromTime, toTime *time.Time

	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, fromStr); err != nil {
			http.Error(w, "invalid 'from' time format. Use RFC3339, e.g. 2006-01-02T15:04:05.000Z", http.StatusBadRequest)
			return
		} else {
			fromTime = &parsed
		}
	}

	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, toStr); err != nil {
			http.Error(w, "invalid 'to' time format. Use RFC3339, e.g. 2006-01-02T15:04:05.000Z", http.StatusBadRequest)
			return
		} else {
			toTime = &parsed
		}
	}

	summary, err := h.paymentService.GetPaymentsSummary(fromTime, toTime)
	if err != nil {
		log.Printf("error getting payment summary: %v", err)
		http.Error(w, "error retrieving payment summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(summary); err != nil {
		log.Printf("error encoding JSON response: %v", err)
		http.Error(w, "error encoding response", http.StatusInternalServerError)
		return
	}
}

func (h *PaymentHandler) HandlePurgePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.paymentService.PurgePayments(); err != nil {
		log.Printf("Error purging payments: %v", err)
		http.Error(w, "Error purging payments", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
