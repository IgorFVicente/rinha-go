package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Server configuration
	Port string

	// Redis configuration
	RedisAddr string

	// Payment processor URLs
	DefaultProcessorURL  string
	FallbackProcessorURL string

	// Health check configuration
	HealthCheckInterval time.Duration

	// Queue configuration
	PaymentQueue      string
	RetryDelayQueue   string
	PaymentsKeyPrefix string
	PaymentsIndexKey  string

	WorkerCount int
}

func Load() *Config {
	return &Config{
		Port:                 getEnv("PORT", "8080"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		DefaultProcessorURL:  getEnv("DEFAULT_PROCESSOR_URL", "http://payment-processor-default:8080"),
		FallbackProcessorURL: getEnv("FALLBACK_PROCESSOR_URL", "http://payment-processor-fallback:8080"),
		HealthCheckInterval:  getDurationEnv("HEALTH_CHECK_INTERVAL", 5*time.Second),
		PaymentQueue:         getEnv("PAYMENT_QUEUE", "payment_jobs"),
		RetryDelayQueue:      getEnv("RETRY_DELAY_QUEUE", "payment_jobs_delayed"),
		PaymentsKeyPrefix:    getEnv("PAYMENTS_KEY_PREFIX", "payment:"),
		PaymentsIndexKey:     getEnv("PAYMENTS_INDEX_KEY", "payments_index"),
		WorkerCount:          getIntEnv("WORKER_COUNT", 20),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
