package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"payment-service/internal/models"
)

type RedisRepository struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisRepository(addr string) (*RedisRepository, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     100,
		MinIdleConns: 10,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	log.Println("Redis connection initialized successfully")
	return &RedisRepository{
		client: rdb,
		ctx:    ctx,
	}, nil
}

func (r *RedisRepository) StorePayment(correlationId string, amount float64, processor string, requestedAt time.Time, keyPrefix, indexKey string) error {
	payment := models.PaymentRecord{
		CorrelationID: correlationId,
		Amount:        amount,
		Processor:     processor,
		RequestedAt:   requestedAt,
	}

	paymentData, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("failed to marshal payment: %v", err)
	}

	paymentKey := fmt.Sprintf("%s%s", keyPrefix, correlationId)

	err = r.client.Set(r.ctx, paymentKey, paymentData, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %v", err)
	}

	err = r.client.SAdd(r.ctx, indexKey, correlationId).Err()
	if err != nil {
		return fmt.Errorf("failed to add payment to index: %v", err)
	}

	log.Printf("Payment stored: %s, %.2f, %s", correlationId, amount, processor)
	return nil
}

func (r *RedisRepository) GetAllPayments(keyPrefix, indexKey string) ([]models.PaymentRecord, error) {
	correlationIDs, err := r.client.SMembers(r.ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get correlation IDs: %v", err)
	}

	var payments []models.PaymentRecord

	for _, correlationID := range correlationIDs {
		paymentKey := fmt.Sprintf("%s%s", keyPrefix, correlationID)
		paymentData, err := r.client.Get(r.ctx, paymentKey).Result()
		if err != nil {
			log.Printf("Warning: failed to get payment %s: %v", paymentKey, err)
			continue
		}

		var payment models.PaymentRecord
		if err := json.Unmarshal([]byte(paymentData), &payment); err != nil {
			log.Printf("Warning: failed to unmarshal payment %s: %v", paymentKey, err)
			continue
		}

		payments = append(payments, payment)
	}

	return payments, nil
}

func (r *RedisRepository) PurgePayments(keyPrefix, indexKey string) error {
	// Get all correlation IDs
	correlationIDs, err := r.client.SMembers(r.ctx, indexKey).Result()
	if err != nil {
		return fmt.Errorf("error getting correlation IDs for purge: %v", err)
	}

	// Delete all payment records
	for _, correlationID := range correlationIDs {
		paymentKey := fmt.Sprintf("%s%s", keyPrefix, correlationID)
		if err := r.client.Del(r.ctx, paymentKey).Err(); err != nil {
			log.Printf("Warning: failed to delete payment %s: %v", paymentKey, err)
		}
	}

	// Clear the index
	if err := r.client.Del(r.ctx, indexKey).Err(); err != nil {
		return fmt.Errorf("error clearing payments index: %v", err)
	}

	log.Println("All payments purged from Redis")
	return nil
}

func (r *RedisRepository) EnqueueJob(job models.PaymentJob, queueName string) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshall job: %v", err)
	}

	err = r.client.LPush(r.ctx, queueName, jobData).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %v", err)
	}

	log.Printf("Job enqueued: %s", job.CorrelationId)
	return nil
}

func (r *RedisRepository) EnqueueDelayedJob(job models.PaymentJob, delay time.Duration, delayQueueName string) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshall delayed job: %v", err)
	}

	score := float64(time.Now().Add(delay).Unix())
	err = r.client.ZAdd(r.ctx, delayQueueName, redis.Z{
		Score:  score,
		Member: jobData,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to enqueue delayed job: %v", err)
	}

	log.Printf("Delayed job enqueued: %s, retry in %v", job.CorrelationId, delay)
	return nil
}

func (r *RedisRepository) DequeueJob(queueName string) (models.PaymentJob, error) {
	result, err := r.client.BRPop(r.ctx, 0, queueName).Result()
	if err != nil {
		return models.PaymentJob{}, err
	}

	jobData := result[1]
	var job models.PaymentJob
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return models.PaymentJob{}, fmt.Errorf("error unmarshaling job: %v", err)
	}

	return job, nil
}

func (r *RedisRepository) GetDelayedJobs(delayQueueName string) ([]models.PaymentJob, error) {
	now := float64(time.Now().Unix())

	result, err := r.client.ZRangeByScoreWithScores(r.ctx, delayQueueName, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatFloat(now, 'f', 0, 64),
	}).Result()

	if err != nil {
		return nil, err
	}

	var jobs []models.PaymentJob
	for _, z := range result {
		jobData := z.Member.(string)

		var job models.PaymentJob
		if err := json.Unmarshal([]byte(jobData), &job); err != nil {
			log.Printf("Error unmarshaling delayed job: %v", err)
			continue
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (r *RedisRepository) MoveDelayedJobToMainQueue(job models.PaymentJob, mainQueueName, delayQueueName string) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return err
	}

	err = r.client.LPush(r.ctx, mainQueueName, jobData).Err()
	if err != nil {
		return fmt.Errorf("error moving delayed job to main queue: %v", err)
	}

	r.client.ZRem(r.ctx, delayQueueName, jobData)
	return nil
}
