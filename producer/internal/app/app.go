package app

import (
	"fmt"

	"vinnipuh1982/producer/internal/config"
)

// Run is the main application entry point invoked by cmd/producer.
func Run(cfg config.Config) error {
	// Here you can initialize logging, metrics, DI, Kafka producer, etc.
	fmt.Printf("Producer starting (env=%s)\n", cfg.Env)
	// No-op for now.
	return nil
}
