FROM golang:1.24-alpine AS builder

RUN apk add --no-cache 

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o payment-service ./cmd/server

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/payment-service .

EXPOSE 8080

CMD ["./payment-service"]