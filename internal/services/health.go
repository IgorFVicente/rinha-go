package services

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/models"
)

type HealthService struct {
	cfg               *config.Config
	mu                sync.RWMutex
	defaultProcessor  models.ServiceHealth
	fallbackProcessor models.ServiceHealth
	lastUpdated       time.Time
	ticker            *time.Ticker
	stopChan          chan struct{}
}

func NewHealthService(cfg *config.Config) *HealthService {
	return &HealthService{
		cfg:               cfg,
		stopChan:          make(chan struct{}),
		defaultProcessor:  models.ServiceHealth{Failing: false},
		fallbackProcessor: models.ServiceHealth{Failing: false},
	}
}

func (h *HealthService) Start() {
	// Initial health check
	h.updateHealthCache()

	// Start periodic health checking for fallback only
	// Default processor health is managed by payment service
	h.ticker = time.NewTicker(h.cfg.HealthCheckInterval)
	go func() {
		for {
			select {
			case <-h.ticker.C:
				h.updateFallbackHealth()
			case <-h.stopChan:
				return
			}
		}
	}()
}

func (h *HealthService) Stop() {
	if h.ticker != nil {
		h.ticker.Stop()
	}
	close(h.stopChan)
}

// Methods called by payment service to manage default processor health
func (h *HealthService) MarkDefaultProcessorHealthy() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.defaultProcessor.Failing {
		h.defaultProcessor.Failing = false
		log.Printf("Default processor marked as HEALTHY by payment service")
	}
}

func (h *HealthService) MarkDefaultProcessorUnhealthy() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.defaultProcessor.Failing {
		h.defaultProcessor.Failing = true
		log.Printf("Default processor marked as UNHEALTHY by payment service")
	}
}

// Method for payment service to check fallback health
func (h *HealthService) IsFallbackProcessorHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.fallbackProcessor.Failing
}

func (h *HealthService) GetHealthyService() (string, error) {
	h.mu.RLock()
	defaultProcessorFailing := h.defaultProcessor.Failing
	fallbackProcessorFailing := h.fallbackProcessor.Failing
	h.mu.RUnlock()

	if !defaultProcessorFailing {
		return h.cfg.DefaultProcessorURL, nil
	}

	if !fallbackProcessorFailing {
		return h.cfg.FallbackProcessorURL, nil
	}

	return "", fmt.Errorf("both services are failing")
}

func (h *HealthService) updateHealthCache() {
	log.Println("Updating health cache...")

	var wg sync.WaitGroup
	var defaultProcessorHealth, fallbackProcessorHealth models.ServiceHealth
	var err1, err2 error

	wg.Add(2)

	go func() {
		defer wg.Done()
		defaultProcessorHealth, err1 = h.checkServiceHealth(h.cfg.DefaultProcessorURL)
		if err1 != nil {
			log.Printf("Error checking defaultProcessor health: %v", err1)
			defaultProcessorHealth = models.ServiceHealth{Failing: true}
		}
	}()

	go func() {
		defer wg.Done()
		fallbackProcessorHealth, err2 = h.checkServiceHealth(h.cfg.FallbackProcessorURL)
		if err2 != nil {
			log.Printf("Error checking fallbackProcessor health: %v", err2)
			fallbackProcessorHealth = models.ServiceHealth{Failing: true}
		}
	}()

	wg.Wait()

	h.mu.Lock()
	h.defaultProcessor = defaultProcessorHealth
	h.fallbackProcessor = fallbackProcessorHealth
	h.lastUpdated = time.Now()
	h.mu.Unlock()

	log.Printf("Initial health cache - Default failing: %t, Fallback failing: %t",
		defaultProcessorHealth.Failing, fallbackProcessorHealth.Failing)
}

func (h *HealthService) updateFallbackHealth() {
	fallbackHealth, err := h.checkServiceHealth(h.cfg.FallbackProcessorURL)
	if err != nil {
		log.Printf("Error checking fallback processor health: %v", err)
		fallbackHealth = models.ServiceHealth{Failing: true}
	}

	h.mu.Lock()
	oldStatus := h.fallbackProcessor.Failing
	h.fallbackProcessor = fallbackHealth
	h.lastUpdated = time.Now()
	h.mu.Unlock()

	if oldStatus != fallbackHealth.Failing {
		status := "HEALTHY"
		if fallbackHealth.Failing {
			status = "UNHEALTHY"
		}
		log.Printf("Fallback processor status changed to %s by health service", status)
	}
}

func (h *HealthService) checkServiceHealth(serviceUrl string) (models.ServiceHealth, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(serviceUrl + "/payments/service-health")
	if err != nil {
		return models.ServiceHealth{Failing: true}, err
	}
	defer resp.Body.Close()

	var health models.ServiceHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return models.ServiceHealth{Failing: true}, err
	}

	return health, nil
}
