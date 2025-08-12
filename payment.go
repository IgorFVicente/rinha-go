package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/redis/go-redis/v9"
)

var (
	healthCache = &HealthCache{}
	rdb         *redis.Client
	ctx         = context.Background()
)

const (
	DEFAULT_PROCESSOR_URL  = "http://payment-processor-default:8080"
	FALLBACK_PROCESSOR_URL = "http://payment-processor-fallback:8080"
	HEALTH_CHECK_INTERVAL  = 5 * time.Second
	PAYMENT_QUEUE          = "payment_jobs"
	RETRY_DELAY_QUEUE      = "payment_jobs_delayed"
	PAYMENTS_KEY_PREFIX    = "payment:"
	PAYMENTS_INDEX_KEY     = "payments_index"
	MAX_RETRIES            = 3
	RETRY_DELAY            = 2 * time.Second
	WORKER_COUNT           = 39
)

func setupLogging() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// logFile, err := os.OpenFile("payment-service.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open log file: %v", err)
	// }

	// multiWriter := io.MultiWriter(os.Stdout, logFile)

	// log.SetOutput(multiWriter)

	// log.SetFlags(log.LstdFlags | log.Lshortfile)

	// return logFile, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func initRedis() error {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %v", err)
	}

	log.Println("Redis connection initialized succesfully")
	return nil
}

// func initDatabase() error {
// 	var err error
// 	db, err = sql.Open("sqlite3", "./payments.db?_busy_timeout=30000&_journal_mode=WAL")
// 	if err != nil {
// 		return fmt.Errorf("failed to open database: %v", err)
// 	}

// 	createTableSQL := `
// 	CREATE TABLE IF NOT EXISTS payments (
// 		id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		correlation_id TEXT NOT NULL,
// 		amount REAL NOT NULL,
// 		processor TEXT NOT NULL,
// 		requested_at DATETIME NOT NULL
// 	);`

// 	if _, err := db.Exec(createTableSQL); err != nil {
// 		return fmt.Errorf("failed to create table: %v", err)
// 	}

// 	log.Println("Database initialized successfully")
// 	return nil
// }

// func storePayment(correlationId string, amount float64, processor string, requestedAt time.Time) error {
// 	insertSQL := `
// 	INSERT INTO payments (correlation_id, amount, processor, requested_at)
// 	VALUES (?, ?, ?, ?)`

// 	_, err := db.Exec(insertSQL, correlationId, amount, processor, requestedAt)

// 	if err != nil {
// 		return fmt.Errorf("failed to store payment: %v", err)
// 	}

// 	log.Printf("Payment stored: %s, %.2f, %s", correlationId, amount, processor)
// 	return nil
// }

func storePayment(correlationId string, amount float64, processor string, requestedAt time.Time) error {
	payment := PaymentRecord{
		CorrelationID: correlationId,
		Amount:        amount,
		Processor:     processor,
		RequestedAt:   requestedAt,
	}

	paymentData, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("failed to marshal payment: %v", err)
	}

	paymentKey := fmt.Sprintf("%s%s", PAYMENTS_KEY_PREFIX, correlationId)
	err = rdb.Set(ctx, paymentKey, paymentData, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %v", err)
	}

	err = rdb.SAdd(ctx, PAYMENTS_INDEX_KEY, correlationId).Err()
	if err != nil {
		return fmt.Errorf("failed to add payment to index: %v", err)
	}

	log.Printf("Payment stored: %s, %.2f, %s", correlationId, amount, processor)
	return nil
}

// func getPaymentsSummary(fromTime, toTime *time.Time) (PaymentSummaryResponse, error) {
// 	var response PaymentSummaryResponse

// 	baseQuery := `
// 	SELECT
// 		processor,
// 		COUNT (*) as total_requests,
// 		COALESCE(SUM(amount), 0) as total_amount
// 	FROM payments
// 	WHERE 1=1`

// 	args := []interface{}{}

// 	if fromTime != nil {
// 		baseQuery += " AND requested_at >= ?"
// 		args = append(args, fromTime.Format("2006-01-02 15:04:05.000+00:00"))
// 	}

// 	if toTime != nil {
// 		baseQuery += " AND requested_at <= ?"
// 		args = append(args, toTime.Format("2006-01-02 15:04:05.000+00:00"))
// 	}

// 	baseQuery += " GROUP BY processor"

// 	rows, err := db.Query(baseQuery, args...)

// 	log.Printf("TESTE IGOR")
// 	log.Println(baseQuery)
// 	log.Println(args)
// 	log.Println(rows)

// 	if err != nil {
// 		return response, fmt.Errorf("failed to query payments: %v", err)
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var processor string
// 		var totalRequests int
// 		var totalAmount float64

// 		if err := rows.Scan(&processor, &totalRequests, &totalAmount); err != nil {
// 			return response, fmt.Errorf("failed to scan row: %v", err)
// 		}

// 		summary := PaymentSummary{
// 			TotalRequests: totalRequests,
// 			TotalAmount:   totalAmount,
// 		}

// 		if processor == "default" {
// 			response.Default = summary
// 		} else if processor == "fallback" {
// 			response.Fallback = summary
// 		}
// 	}

// 	return response, nil
// }

func getAllPayments() ([]PaymentRecord, error) {
	correlationIDs, err := rdb.SMembers(ctx, PAYMENTS_INDEX_KEY).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get correlation IDs: %v", err)
	}

	var payments []PaymentRecord

	for _, correlationID := range correlationIDs {
		paymentKey := fmt.Sprintf("%s%s", PAYMENTS_KEY_PREFIX, correlationID)
		paymentData, err := rdb.Get(ctx, paymentKey).Result()
		if err != nil {
			log.Printf("Warning: failed to get payment %s: %v", paymentKey, err)
			continue
		}

		var payment PaymentRecord
		if err := json.Unmarshal([]byte(paymentData), &payment); err != nil {
			log.Printf("Warning: failed to unmarshal payment %s: %v", paymentKey, err)
			continue
		}

		payments = append(payments, payment)
	}

	return payments, nil
}

func getPaymentsSummary(fromTime, toTime *time.Time) (PaymentSummaryResponse, error) {
	var response PaymentSummaryResponse

	payments, err := getAllPayments()
	if err != nil {
		return response, err
	}

	var filteredPayments []PaymentRecord
	for _, payment := range payments {
		include := true

		fmt.Println(payment, fromTime, toTime)
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

	defaultSummary := PaymentSummary{}
	fallbackSummary := PaymentSummary{}

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

func checkServiceHealth(serviceUrl string) (ServiceHealth, error) {
	resp, err := http.Get(serviceUrl + "/payments/service-health")
	if err != nil {
		return ServiceHealth{Failing: true}, err
	}
	defer resp.Body.Close()

	var health ServiceHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return ServiceHealth{Failing: true}, err
	}

	return health, nil
}

func updateHealthCache() {
	log.Println("Updating health cache...")

	var wg sync.WaitGroup
	var defaultProcessorHealth, fallbackProcessorHealth ServiceHealth
	var err1, err2 error

	wg.Add(2)

	go func() {
		defer wg.Done()
		defaultProcessorHealth, err1 = checkServiceHealth(DEFAULT_PROCESSOR_URL)
		if err1 != nil {
			log.Printf("Error checking defaultProcessor health: %v", err1)
			defaultProcessorHealth = ServiceHealth{Failing: true}
		}
	}()

	go func() {
		defer wg.Done()
		fallbackProcessorHealth, err2 = checkServiceHealth(FALLBACK_PROCESSOR_URL)
		if err2 != nil {
			log.Printf("Error checking defaultProcessor health: %v", err2)
			fallbackProcessorHealth = ServiceHealth{Failing: true}
		}
	}()

	wg.Wait()

	healthCache.mu.Lock()
	healthCache.defaultProcessor = defaultProcessorHealth
	healthCache.fallbackProcessor = fallbackProcessorHealth
	healthCache.lastUpdated = time.Now()
	healthCache.mu.Unlock()

	log.Printf("Health cache updated - Service1 failing: %t, Service2 failing: %t", defaultProcessorHealth.Failing, fallbackProcessorHealth.Failing)
}

func getHealthyService() (string, error) {
	healthCache.mu.RLock()
	defaultProcessorFailing := healthCache.defaultProcessor.Failing
	fallbackProcessorFailing := healthCache.fallbackProcessor.Failing
	healthCache.mu.RUnlock()

	if !defaultProcessorFailing {
		return DEFAULT_PROCESSOR_URL, nil
	}

	if !fallbackProcessorFailing {
		return FALLBACK_PROCESSOR_URL, nil
	}

	return "", fmt.Errorf("both services are failing")
}

func markServiceAsFailing(serviceURL string) {
	healthCache.mu.Lock()
	defer healthCache.mu.Unlock()

	if serviceURL == DEFAULT_PROCESSOR_URL {
		healthCache.defaultProcessor.Failing = true
		log.Printf("Marked defaultProcessor (%s) as failing in cache", serviceURL)
	} else if serviceURL == FALLBACK_PROCESSOR_URL {
		healthCache.fallbackProcessor.Failing = true
		log.Printf("Marked fallbackProcessor (%s) as failing in cache", serviceURL)
	}
}

func startHealthChecker() {
	updateHealthCache()

	ticker := time.NewTicker(HEALTH_CHECK_INTERVAL)
	go func() {
		for range ticker.C {
			updateHealthCache()
		}
	}()
}

func enqueueJob(job PaymentJob) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshall job: %v", err)
	}

	err = rdb.LPush(ctx, PAYMENT_QUEUE, jobData).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %v", err)
	}

	log.Printf("Job enqueued: %s", job.CorrelationId)
	return nil
}

func enqueuDelayedJob(job PaymentJob, delay time.Duration) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshall delayed job: %v", err)
	}

	score := float64(time.Now().Add(delay).Unix())
	err = rdb.ZAdd(ctx, RETRY_DELAY_QUEUE, redis.Z{
		Score:  score,
		Member: jobData,
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to enqueue delayed job: %v", err)
	}

	log.Printf("Delayed job enqueued: %s, retry in %v", job.CorrelationId, delay)
	return nil
}

func processDelayedJobs() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			now := float64(time.Now().Unix())

			result, err := rdb.ZRangeByScoreWithScores(ctx, RETRY_DELAY_QUEUE, &redis.ZRangeBy{
				Min: "0",
				Max: strconv.FormatFloat(now, 'f', 0, 64),
			}).Result()

			if err != nil {
				log.Printf("Error getting delayed jobs: %v", err)
				continue
			}

			for _, z := range result {
				jobData := z.Member.(string)

				err = rdb.LPush(ctx, PAYMENT_QUEUE, jobData).Err()

				if err != nil {
					log.Printf("Error moving delayed job to main queue: %v", err)
					continue
				}

				rdb.ZRem(ctx, RETRY_DELAY_QUEUE)
				log.Printf("Moved delayed job back to queue")
			}
		}
	}()
}

func forwardPayment(serviceUrl string, data ForwardedPaymentData) error {
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

func getProcessorName(serviceUrl string) string {
	if serviceUrl == DEFAULT_PROCESSOR_URL {
		return "default"
	}

	return "fallback"
}

func processJob(job PaymentJob) error {
	serviceUrl, err := getHealthyService()
	if err != nil {
		log.Printf("No healthy services available for job %s: %v", job.CorrelationId, err)
		return err
	}

	forwardedData := ForwardedPaymentData{
		CorrelationId: job.CorrelationId,
		Amount:        job.Amount,
		RequestedAt:   job.RequestedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if err := forwardPayment(serviceUrl, forwardedData); err != nil {
		log.Printf("Error forwarding payment for job %s: %v", job.CorrelationId, err)
		return err
	}

	processorName := getProcessorName(serviceUrl)
	if err := storePayment(job.CorrelationId, job.Amount, processorName, job.RequestedAt); err != nil {
		log.Printf("Error storing payment for job %s: %v", job.CorrelationId, err)
	}

	log.Printf("Job processed successfully: %s", job.CorrelationId)
	return nil
}

func startWorkers() {
	for i := 0; i < WORKER_COUNT; i++ {
		go func(workerID int) {
			log.Printf("Worker %d started", workerID)

			for {
				result, err := rdb.BRPop(ctx, 0, PAYMENT_QUEUE).Result()
				if err != nil {
					log.Printf("Worker %d: Error getting job from queue: %v", workerID, err)
					time.Sleep(time.Second)
					continue
				}

				jobData := result[1]
				var job PaymentJob
				if err := json.Unmarshal([]byte(jobData), &job); err != nil {
					log.Printf("Worker %d: Error unmarshaling job: %v", workerID, err)
					continue
				}

				log.Printf("Workerr %d: Proessing job %s", workerID, job.CorrelationId)

				if err := processJob(job); err != nil {
					job.RetryCount++

					if job.RetryCount >= job.MaxRetries {
						log.Printf("Worker %d: Job %s exceeded max retries (%d), discarding", workerID, job.CorrelationId, job.MaxRetries)
						continue
					}

					log.Printf("Worker %d: Job %s failed, scheduling retry %d/%d", workerID, job.CorrelationId, job.RetryCount, job.MaxRetries)

					if err := enqueuDelayedJob(job, RETRY_DELAY); err != nil {
						log.Printf("Worker %d: Error enqueuing delayed job: %v", workerID, err)
					}
				}
			}
		}(i + 1)
	}
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

	job := PaymentJob{
		CorrelationId: paymentData.CorrelationId,
		Amount:        paymentData.Amount,
		RequestedAt:   time.Now().UTC().Truncate(time.Millisecond),
		RetryCount:    0,
		MaxRetries:    MAX_RETRIES,
	}

	if err := enqueueJob(job); err != nil {
		log.Printf("Error enqueuing payment job: %v", err)
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func paymentsSummaryHandler(w http.ResponseWriter, r *http.Request) {
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

	summary, err := getPaymentsSummary(fromTime, toTime)
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

// func purgePaymentsHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	_, err := db.Exec("DELETE FROM payments")
// 	if err != nil {
// 		log.Printf("Error purging payments: %v", err)
// 		http.Error(w, "Error purging payments", http.StatusInternalServerError)
// 		return
// 	}

// 	log.Println("All payments purged from database")
// 	w.WriteHeader(http.StatusOK)
// }

func purgePaymentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get all correlation IDs
	correlationIDs, err := rdb.SMembers(ctx, PAYMENTS_INDEX_KEY).Result()
	if err != nil {
		log.Printf("Error getting correlation IDs for purge: %v", err)
		http.Error(w, "Error purging payments", http.StatusInternalServerError)
		return
	}

	// Delete all payment records
	for _, correlationID := range correlationIDs {
		paymentKey := fmt.Sprintf("%s%s", PAYMENTS_KEY_PREFIX, correlationID)
		if err := rdb.Del(ctx, paymentKey).Err(); err != nil {
			log.Printf("Warning: failed to delete payment %s: %v", paymentKey, err)
		}
	}

	// Clear the index
	if err := rdb.Del(ctx, PAYMENTS_INDEX_KEY).Err(); err != nil {
		log.Printf("Error clearing payments index: %v", err)
		http.Error(w, "Error purging payments", http.StatusInternalServerError)
		return
	}

	log.Println("All payments purged from Redis")
	w.WriteHeader(http.StatusOK)
}

func main() {
	// logFile, err := setupLogging()

	// if err != nil {
	// 	log.Fatalf("failed to set up logging: %v", err)
	// }

	// defer logFile.Close()

	setupLogging()

	if err := initRedis(); err != nil {
		log.Fatalf("failed to initialize Redis: %v", err)
	}

	// if err := initDatabase(); err != nil {
	// 	log.Fatalf("failed to initialize database: %v", err)
	// }
	// defer db.Close()

	startHealthChecker()
	startWorkers()
	processDelayedJobs()

	http.HandleFunc("/payments", paymentsHandler)
	http.HandleFunc("/payments-summary", paymentsSummaryHandler)
	http.HandleFunc("/purge-payments", purgePaymentsHandler)

	fmt.Println("Server starting on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
