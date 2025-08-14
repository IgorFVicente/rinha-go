package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
		Addr:            addr,
		PoolSize:        500,
		MinIdleConns:    20,
		PoolTimeout:     2 * time.Second,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    1 * time.Second,
		MaxRetries:      1,
		MaxRetryBackoff: 256 * time.Millisecond,
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

	pipe := r.client.Pipeline()
	pipe.Set(r.ctx, paymentKey, paymentData, 0)
	pipe.SAdd(r.ctx, indexKey, correlationId)

	_, err = pipe.Exec(r.ctx)
	if err != nil {
		return fmt.Errorf("failed to store payment: %v", err)
	}

	// log.Printf("Payment stored: %s, %.2f, %s", correlationId, amount, processor)
	return nil
}

func (r *RedisRepository) GetAllPayments(keyPrefix, indexKey string) ([]models.PaymentRecord, error) {
	correlationIDs, err := r.client.SMembers(r.ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get correlation IDs: %v", err)
	}

	var payments []models.PaymentRecord

	// Use pipeline to fetch all payments at once for better performance
	if len(correlationIDs) > 0 {
		pipe := r.client.Pipeline()
		cmds := make(map[string]*redis.StringCmd)

		for _, correlationID := range correlationIDs {
			paymentKey := fmt.Sprintf("%s%s", keyPrefix, correlationID)
			cmds[correlationID] = pipe.Get(r.ctx, paymentKey)
		}

		_, err = pipe.Exec(r.ctx)
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get payments: %v", err)
		}

		for correlationID, cmd := range cmds {
			paymentData, err := cmd.Result()
			if err != nil {
				log.Printf("Warning: failed to get payment %s: %v", correlationID, err)
				continue
			}

			var payment models.PaymentRecord
			if err := json.Unmarshal([]byte(paymentData), &payment); err != nil {
				log.Printf("Warning: failed to unmarshal payment %s: %v", correlationID, err)
				continue
			}

			payments = append(payments, payment)
		}
	}

	return payments, nil
}

func (r *RedisRepository) PurgePayments(keyPrefix, indexKey string) error {
	correlationIDs, err := r.client.SMembers(r.ctx, indexKey).Result()
	if err != nil {
		return fmt.Errorf("error getting correlation IDs for purge: %v", err)
	}

	if len(correlationIDs) > 0 {
		// Use pipeline for batch deletion
		pipe := r.client.Pipeline()

		for _, correlationID := range correlationIDs {
			paymentKey := fmt.Sprintf("%s%s", keyPrefix, correlationID)
			pipe.Del(r.ctx, paymentKey)
		}

		pipe.Del(r.ctx, indexKey)

		_, err = pipe.Exec(r.ctx)
		if err != nil {
			return fmt.Errorf("error purging payments: %v", err)
		}
	} else {
		r.client.Del(r.ctx, indexKey)
	}

	log.Println("All payments purged from Redis")
	return nil
}

func (r *RedisRepository) EnqueueJob(rawJSON []byte, queueName string) error {
	return r.client.LPush(r.ctx, queueName, rawJSON).Err()
}

func (r *RedisRepository) DequeueJob(queueName string) ([]byte, error) {
	result, err := r.client.BRPop(r.ctx, 0, queueName).Result()
	if err != nil {
		return nil, err
	}
	return []byte(result[1]), nil
}
