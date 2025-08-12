package models

import "time"

type PaymentData struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentJob struct {
	CorrelationId string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
	RetryCount    int       `json:"retryCount"`
	MaxRetries    int       `json:"maxRetries"`
}

type ForwardedPaymentData struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

type ServiceHealth struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type PaymentSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type PaymentSummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

type PaymentRecord struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	Processor     string    `json:"processor"`
	RequestedAt   time.Time `json:"requestedAt"`
}
