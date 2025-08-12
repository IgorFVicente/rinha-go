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
		if payment.Processor == "default" {
			defaultSummary.TotalRequests++
			defaultSummary.TotalAmount += payment.Amount
		} else if payment.Processor == "fallback" {
			fallbackSummary.TotalRequests++
			fallbackSummary.TotalAmount += payment.Amount
		}
	}

	response.Default = defaultSummary
	response.Fallback = fallbackSummary

	log.Printf("Summary - Default: %d requests, %.2f total. Fallback: %d requests, %.2f total",
		defaultSummary.TotalRequests, defaultSummary.TotalAmount,
		fallbackSummary.TotalRequests, fallbackSummary.TotalAmount)

	return response, nil
}

func (p *PaymentService) PurgePayments() error {
	return p.repo.PurgePayments(p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey)
}

func (p *PaymentService) ProcessPayment(job models.PaymentJob) error {
	serviceUrl, err := p.healthService.GetHealthyService()
	if err != nil {
		log.Printf("No healthy services available for job %s: %v", job.CorrelationId, err)
		return err
	}

	forwardedData := models.ForwardedPaymentData{
		CorrelationId: job.CorrelationId,
		Amount:        job.Amount,
		RequestedAt:   job.RequestedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if err := p.forwardPayment(serviceUrl, forwardedData); err != nil {
		log.Printf("Error forwarding payment for job %s: %v", job.CorrelationId, err)
		p.healthService.MarkServiceAsFailing(serviceUrl)
		return err
	}

	processorName := p.getProcessorName(serviceUrl)
	if err := p.repo.StorePayment(job.CorrelationId, job.Amount, processorName, job.RequestedAt, p.cfg.PaymentsKeyPrefix, p.cfg.PaymentsIndexKey); err != nil {
		log.Printf("Error storing payment for job %s: %v", job.CorrelationId, err)
	}

	log.Printf("Job processed successfully: %s", job.CorrelationId)
	return nil
}

func (p *PaymentService) forwardPayment(serviceUrl string, data models.ForwardedPaymentData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	log.Printf("Sending to %s/payments: %s", serviceUrl, string(jsonData))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(serviceUrl+"/payments", "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("Response from %s: status %d", serviceUrl, resp.StatusCode)

	if resp.StatusCode >= 400 {
		responseBody, _ := io.ReadAll(resp.Body)
		log.Printf("Error response from %s: %s", serviceUrl, string(responseBody))
		return fmt.Errorf("service returned status %d", resp.StatusCode)
	}

	return nil
}

func (p *PaymentService) getProcessorName(serviceUrl string) string {
	if serviceUrl == p.cfg.DefaultProcessorURL {
		return "default"
	}
	return "fallback"
}
