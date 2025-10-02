package config

import (
	"os"
	"strings"
)

// Config holds application runtime configuration.
type Config struct {
	Env          string   // e.g. development, staging, production
	KafkaBrokers []string // Kafka broker addresses
	AppName      string
}

// Load constructs Config from environment variables with sane defaults.
func Load() Config {
	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersStr, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	return Config{
		Env:          getEnv("APP_ENV", "development"),
		KafkaBrokers: brokers,
		AppName:      getEnv("APP_NAME", "producer"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
