package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"payment-service/internal/config"
	"payment-service/internal/handlers"
	"payment-service/internal/repository"
	"payment-service/internal/services"
	"payment-service/internal/utils"

	"github.com/valyala/fasthttp"
)

func main() {
	if gomaxprocs := os.Getenv("GOMAXPROCS"); gomaxprocs != "" {
		if n, err := strconv.Atoi(gomaxprocs); err == nil {
			runtime.GOMAXPROCS(n)
		}
	}

	utils.SetupLogging()

	cfg := config.Load()

	repo, err := repository.NewRedisRepository(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to initialize Redis repository: %v", err)
	}

	healthService := services.NewHealthService(cfg)
	queueService := services.NewQueueService(repo, cfg)
	paymentService := services.NewPaymentService(repo, healthService, cfg)

	healthService.Start()
	queueService.StartWorkers(paymentService)

	paymentHandler := handlers.NewPaymentHandler(paymentService, queueService)

	router := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		method := string(ctx.Method())

		switch {
		case method == "POST" && path == "/payments":
			paymentHandler.HandlePayments(ctx)
		case method == "GET" && path == "/payments-summary":
			paymentHandler.HandlePaymentsSummary(ctx)
		case method == "POST" && path == "/purge-payments":
			paymentHandler.HandlePurgePayments(ctx)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			ctx.SetBodyString("Not Found")
		}
	}

	server := &fasthttp.Server{
		Handler:      router,
		IdleTimeout:  60 * time.Second,
		TCPKeepalive: true,
	}

	var listener net.Listener
	if sock := os.Getenv("SOCK"); sock != "" {
		os.Remove(sock)
		listener, err = net.Listen("unix", sock)
		if err != nil {
			log.Fatalf("Failed to create Unix socket listener: %v", err)
		}
		if err := os.Chmod(sock, 0666); err != nil {
			log.Fatalf("Failed to set socket permissions: %v", err)
		}
		log.Printf("Server starting on Unix socket: %s", sock)
	} else {
		listener, err = net.Listen("tcp", ":"+cfg.Port)
		if err != nil {
			log.Fatalf("Failed to create TCP listener: %v", err)
		}
		log.Printf("Server starting on TCP port: %s", cfg.Port)
	}

	// go func() {
	// 	if err := server.Serve(listener); err != nil {
	// 		log.Fatalf("Server failed to start: %v", err)
	// 	}
	// }()

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	if err := server.Shutdown(); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	healthService.Stop()
	log.Println("Server exited")
}
