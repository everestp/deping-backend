.PHONY: dev infra down tidy lint build

## Start local infra (Postgres, Redis, RabbitMQ)
infra:
	docker compose up -d

## Stop local infra
down:
	docker compose down

## Run the backend (requires infra up + .env present)
dev:
	cp -n .env.example .env 2>/dev/null || true
	go run ./main.go

## Build binary
build:
	go build -o bin/depin-backend ./main.go

## Tidy modules
tidy:
	go mod tidy

## Lint (requires golangci-lint)
lint:
	golangci-lint run ./...

## Apply DB schema
migrate:
	psql "$$DATABASE_URL" -f schema.sql
