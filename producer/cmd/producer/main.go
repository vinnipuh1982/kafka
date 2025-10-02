package main

import (
	"log"

	"vinnipuh1982/producer/internal/app"
	"vinnipuh1982/producer/internal/config"
)

func main() {
	cfg := config.Load()

	if err := app.Run(cfg); err != nil {
		log.Fatalf("application error: %v", err)
	}
}
