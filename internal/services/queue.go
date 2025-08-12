package services

import (
	"log"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/models"
	"payment-service/internal/repository"
)

type QueueService struct {
	repo   *repository.RedisRepository
	cfg    *config.Config
	ticker *time.Ticker
}

func NewQueueService(repo *repository.RedisRepository, cfg *config.Config) *QueueService {
	return &QueueService{
		repo: repo,
		cfg:  cfg,
	}
}

func (q *QueueService) EnqueueJob(job models.PaymentJob) error {
	return q.repo.EnqueueJob(job, q.cfg.PaymentQueue)
}

func (q *QueueService) EnqueueDelayedJob(job models.PaymentJob, delay time.Duration) error {
	return q.repo.EnqueueDelayedJob(job, delay, q.cfg.RetryDelayQueue)
}

func (q *QueueService) StartWorkers(paymentService *PaymentService) {
	for i := 0; i < q.cfg.WorkerCount; i++ {
		go q.worker(i+1, paymentService)
	}
}

func (q *QueueService) StartDelayedJobProcessor() {
	q.ticker = time.NewTicker(5 * time.Second)
	go func() {
		for range q.ticker.C {
			q.processDelayedJobs()
		}
	}()
}

func (q *QueueService) worker(workerID int, paymentService *PaymentService) {
	log.Printf("Worker %d started", workerID)

	for {
		job, err := q.repo.DequeueJob(q.cfg.PaymentQueue)
		if err != nil {
			log.Printf("Worker %d: Error getting job from queue: %v", workerID, err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf("Worker %d: Processing job %s", workerID, job.CorrelationId)

		if err := paymentService.ProcessPayment(job); err != nil {
			job.RetryCount++

			if job.RetryCount >= job.MaxRetries {
				log.Printf("Worker %d: Job %s exceeded max retries (%d), discarding",
					workerID, job.CorrelationId, job.MaxRetries)
				continue
			}

			log.Printf("Worker %d: Job %s failed, scheduling retry %d/%d",
				workerID, job.CorrelationId, job.RetryCount, job.MaxRetries)

			if err := q.EnqueueDelayedJob(job, q.cfg.RetryDelay); err != nil {
				log.Printf("Worker %d: Error enqueuing delayed job: %v", workerID, err)
			}
		}
	}
}

func (q *QueueService) processDelayedJobs() {
	jobs, err := q.repo.GetDelayedJobs(q.cfg.RetryDelayQueue)
	if err != nil {
		log.Printf("Error getting delayed jobs: %v", err)
		return
	}

	for _, job := range jobs {
		if err := q.repo.MoveDelayedJobToMainQueue(job, q.cfg.PaymentQueue, q.cfg.RetryDelayQueue); err != nil {
			log.Printf("Error moving delayed job to main queue: %v", err)
			continue
		}
		log.Printf("Moved delayed job back to queue: %s", job.CorrelationId)
	}
}
