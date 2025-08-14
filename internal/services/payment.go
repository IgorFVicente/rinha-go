package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/models"
	"payment-service/internal/repository"
)

type PaymentService struct {
	repo          *repository.RedisRepository
	healthService *HealthService
	cfg           *config.Config
}

func NewPaymentService(repo *repository.RedisRepository, healthService *HealthService, cfg *config.Config) *PaymentService {
	return &PaymentService{
		repo:          repo,
		healthService: healthService,
		cfg:           cfg,
	}
}

func (p *PaymentService) ProcessPayment(job models.PaymentJob) error {

	if err := p.tryProcessWithDefault(job); err == nil {
		p.healthService.MarkDefaultProcessorHealthy()
		return nil
	}
	p.healthService.MarkDefaultProcessorUnhealthy()

	if p.healthService.IsFallbackProcessorHealthy() {
		if err := p.tryProcessWithFallback(job); err == nil {
			return nil
		}
	}

	return fmt.Errorf("both processors failed or unavailable")
}

func (p *PaymentService) tryProcessWithDefault(job models.PaymentJob) error {
	forwardedData := models.ForwardedPaymentData{
		CorrelationId: job.CorrelationId,
		Amount:        job.Amount,
		RequestedAt:   job.RequestedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if err := p.forwardPayment(p.cfg.DefaultProcessorURL, forwardedData); err != nil {
		return err
	}

	if err := p.repo.StorePayment(job.CorrelationId, job.Amount, "default", job.RequestedAt, p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey); err != nil {
		log.Printf("Error storing payment for job %s: %v", job.CorrelationId, err)
	}

	return nil
}

func (p *PaymentService) tryProcessWithFallback(job models.PaymentJob) error {
	forwardedData := models.ForwardedPaymentData{
		CorrelationId: job.CorrelationId,
		Amount:        job.Amount,
		RequestedAt:   job.RequestedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if err := p.forwardPayment(p.cfg.FallbackProcessorURL, forwardedData); err != nil {
		return err
	}
	if err := p.repo.StorePayment(job.CorrelationId, job.Amount, "fallback", job.RequestedAt, p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey); err != nil {
		log.Printf("Error storing payment for job %s: %v", job.CorrelationId, err)
	}

	return nil
}

func (p *PaymentService) forwardPayment(serviceUrl string, data models.ForwardedPaymentData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(serviceUrl+"/payments", "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		responseBody, _ := io.ReadAll(resp.Body)
		log.Printf("Error response from %s: status %d, body: %s", serviceUrl, resp.StatusCode, string(responseBody))
		return fmt.Errorf("service returned status %d", resp.StatusCode)
	}

	return nil
}

func (p *PaymentService) GetPaymentsSummary(fromTime, toTime *time.Time) (models.PaymentSummaryResponse, error) {
	var response models.PaymentSummaryResponse

	payments, err := p.repo.GetAllPayments(p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey)
	if err != nil {
		return response, err
	}

	var filteredPayments []models.PaymentRecord
	for _, payment := range payments {
		include := true

		if fromTime != nil && payment.RequestedAt.Before(*fromTime) {
			include = false
		}

		if toTime != nil && payment.RequestedAt.After(*toTime) {
			include = false
		}

		if include {
			filteredPayments = append(filteredPayments, payment)
		}
	}

	defaultSummary := models.PaymentSummary{}
	fallbackSummary := models.PaymentSummary{}

	for _, payment := range filteredPayments {
		switch payment.Processor {
		case "default":
			defaultSummary.TotalRequests++
			defaultSummary.TotalAmount += payment.Amount
		case "fallback":
			fallbackSummary.TotalRequests++
			fallbackSummary.TotalAmount += payment.Amount
		}
	}

	response.Default = defaultSummary
	response.Fallback = fallbackSummary

	return response, nil
}

func (p *PaymentService) PurgePayments() error {
	return p.repo.PurgePayments(p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey)
}
