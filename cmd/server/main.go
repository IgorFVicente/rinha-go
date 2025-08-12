package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/handlers"
	"payment-service/internal/repository"
	"payment-service/internal/services"
	"payment-service/internal/utils"
)

func main() {
	// Setup logging
	utils.SetupLogging()

	// Load configuration
	cfg := config.Load()

	// Initialize Redis repository
	repo, err := repository.NewRedisRepository(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to initialize Redis repository: %v", err)
	}

	// Initialize services
	healthService := services.NewHealthService(cfg)
	queueService := services.NewQueueService(repo, cfg)
	paymentService := services.NewPaymentService(repo, healthService, cfg)

	// Start background services
	healthService.Start()
	queueService.StartWorkers(paymentService)
	queueService.StartDelayedJobProcessor()

	// Setup HTTP handlers
	paymentHandler := handlers.NewPaymentHandler(paymentService, queueService)

	// Setup routes
	mux := http.NewServeMux()
	mux.HandleFunc("/payments", paymentHandler.HandlePayments)
	mux.HandleFunc("/payments-summary", paymentHandler.HandlePaymentsSummary)
	mux.HandleFunc("/purge-payments", paymentHandler.HandlePurgePayments)

	// Create server
	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server starting on port %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
