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
		cfg:      cfg,
		stopChan: make(chan struct{}),
	}
}

func (h *HealthService) Start() {
	h.updateHealthCache()

	h.ticker = time.NewTicker(h.cfg.HealthCheckInterval)
	go func() {
		for {
			select {
			case <-h.ticker.C:
				h.updateHealthCache()
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

func (h *HealthService) MarkServiceAsFailing(serviceURL string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if serviceURL == h.cfg.DefaultProcessorURL {
		h.defaultProcessor.Failing = true
		log.Printf("Marked defaultProcessor (%s) as failing in cache", serviceURL)
	} else if serviceURL == h.cfg.FallbackProcessorURL {
		h.fallbackProcessor.Failing = true
		log.Printf("Marked fallbackProcessor (%s) as failing in cache", serviceURL)
	}
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

	log.Printf("Health cache updated - Default failing: %t, Fallback failing: %t",
		defaultProcessorHealth.Failing, fallbackProcessorHealth.Failing)
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
