FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o depin-backend .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/depin-backend .

# ONLY gRPC port
EXPOSE 50051

CMD ["./depin-backend"]