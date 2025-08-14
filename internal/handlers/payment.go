package handlers

import (
	"encoding/json"
	"log"
	"time"

	"payment-service/internal/services"

	"github.com/valyala/fasthttp"
)

type PaymentHandler struct {
	paymentService *services.PaymentService
	queueService   *services.QueueService
}

func NewPaymentHandler(paymentService *services.PaymentService, queueService *services.QueueService) *PaymentHandler {
	return &PaymentHandler{
		paymentService: paymentService,
		queueService:   queueService,
	}
}

func (h *PaymentHandler) HandlePayments(ctx *fasthttp.RequestCtx) {
	if err := h.queueService.EnqueueJob(ctx.PostBody()); err != nil {
		log.Printf("Error enqueuing raw job: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Error processing request")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func (h *PaymentHandler) HandlePaymentsSummary(ctx *fasthttp.RequestCtx) {
	var fromTime, toTime *time.Time

	if fromBytes := ctx.QueryArgs().Peek("from"); len(fromBytes) > 0 {
		fromStr := string(fromBytes)
		if parsed, err := time.Parse(time.RFC3339Nano, fromStr); err != nil {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.SetBodyString("invalid 'from' time format. Use RFC3339, e.g. 2006-01-02T15:04:05.000Z")
			return
		} else {
			fromTime = &parsed
		}
	}

	if toBytes := ctx.QueryArgs().Peek("to"); len(toBytes) > 0 {
		toStr := string(toBytes)
		if parsed, err := time.Parse(time.RFC3339Nano, toStr); err != nil {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.SetBodyString("invalid 'to' time format. Use RFC3339, e.g. 2006-01-02T15:04:05.000Z")
			return
		} else {
			toTime = &parsed
		}
	}

	summary, err := h.paymentService.GetPaymentsSummary(fromTime, toTime)
	if err != nil {
		log.Printf("error getting payment summary: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("error retrieving payment summary")
		return
	}

	jsonData, err := json.Marshal(summary)
	if err != nil {
		log.Printf("error marshaling payment summary: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("error processing payment summary")
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBody(jsonData)
}

func (h *PaymentHandler) HandlePurgePayments(ctx *fasthttp.RequestCtx) {
	if err := h.paymentService.PurgePayments(); err != nil {
		log.Printf("Error purging payments: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Error purging payments")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}
