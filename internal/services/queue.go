package services

import (
	"encoding/json"
	"log"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/models"
	"payment-service/internal/repository"
)

type QueueService struct {
	repo *repository.RedisRepository
	cfg  *config.Config
}

func NewQueueService(repo *repository.RedisRepository, cfg *config.Config) *QueueService {
	return &QueueService{
		repo: repo,
		cfg:  cfg,
	}
}

func (q *QueueService) EnqueueJob(rawJSON []byte) error {
	return q.repo.EnqueueJob(rawJSON, q.cfg.PaymentQueue)
}

func (q *QueueService) StartWorkers(paymentService *PaymentService) {
	for i := 0; i < q.cfg.WorkerCount; i++ {
		go q.worker(i+1, paymentService)
	}
}

func (q *QueueService) worker(workerID int, paymentService *PaymentService) {
	log.Printf("Worker %d started", workerID)

	for {
		rawJob, err := q.repo.DequeueJob(q.cfg.PaymentQueue)
		if err != nil {
			continue
		}

		var paymentData models.PaymentData
		if err := json.Unmarshal(rawJob, &paymentData); err != nil {
			log.Printf("Worker %d: Error unmarshaling job: %v", workerID, err)
			continue
		}

		job := models.PaymentJob{
			CorrelationId: paymentData.CorrelationId,
			Amount:        paymentData.Amount,
			RequestedAt:   time.Now().UTC().Truncate(time.Millisecond),
		}

		if err := paymentService.ProcessPayment(job); err != nil {
			if err := q.EnqueueJob(rawJob); err != nil {
				log.Printf("Worker %d: Error requeuing job %s: %v", workerID, job.CorrelationId, err)
			}
		}
	}
}
