package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type PaymentData struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
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

type HealthCache struct {
	mu                sync.RWMutex
	defaultProcessor  ServiceHealth
	fallbackProcessor ServiceHealth
	lastUpdated       time.Time
}

var healthCache = &HealthCache{}

const (
	DEFAULT_PROCESSOR_URL  = "http://localhost:8001"
	FALLBACK_PROCESSOR_URL = "http://localhost:8002"
	HEALTH_CHECK_INTERVAL  = 5 * time.Second
)

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

func forwardPayment(serviceUrl string, data ForwardedPaymentData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	log.Printf("Sending to %s/payments: %s", serviceUrl, string(jsonData))

	resp, err := http.Post(serviceUrl+"/payments", "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("Response from %s: status %d", serviceUrl, resp.StatusCode)

	if resp.StatusCode >= 400 {
		responseBody, _ := io.ReadAll(resp.Body)
		log.Printf("Error response from %s: %s", serviceUrl, string(responseBody))
		return fmt.Errorf("Service returned status %d", resp.StatusCode)
	}

	return nil
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

	forwardedData := ForwardedPaymentData{
		CorrelationId: paymentData.CorrelationId,
		Amount:        paymentData.Amount,
		RequestedAt:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	serviceURL, err := getHealthyService()

	if err != nil {
		log.Printf("No healthy services available: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}

	if err := forwardPayment(serviceURL, forwardedData); err != nil {
		log.Printf("Error forwarding payment: %v", err)
		markServiceAsFailing(serviceURL)

		retryServiceURL, retryErr := getHealthyService()
		if retryErr != nil {
			log.Printf("No healthy services available for retry: %v", retryErr)
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}

		// If we got the same service URL, it means both are failing
		if retryServiceURL == serviceURL {
			log.Printf("No alternative service available")
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}

		// Retry with the alternative service
		log.Printf("Retrying payment with alternative service: %s", retryServiceURL)
		if err := forwardPayment(retryServiceURL, forwardedData); err != nil {
			log.Printf("Error forwarding payment to alternative service %s: %v", retryServiceURL, err)
			markServiceAsFailing(retryServiceURL)
			http.Error(w, "Error processing payment", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	startHealthChecker()
	http.HandleFunc("/payments", paymentsHandler)

	fmt.Println("Server starting on port 9999")
	log.Fatal(http.ListenAndServe(":9999", nil))
}
