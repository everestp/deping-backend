package main

import (
	"log"

	"github.com/everestp/depin-backend/app"
	"github.com/everestp/depin-backend/config/env"
)

func main() {
	cfg := env.Load()

	application, err := app.New(cfg)
	if err != nil {
		log.Fatalf("failed to initialize application: %v", err)
	}

	if err := application.Run(); err != nil {
		log.Fatalf("application error: %v", err)
	}
}
