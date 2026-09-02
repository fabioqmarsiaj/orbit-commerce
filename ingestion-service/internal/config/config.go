// Package config loads runtime configuration for the ingestion-service from
// environment variables, with sensible local-development defaults.
package config

import (
	"os"
	"strconv"
)

// Config holds the runtime configuration for the ingestion-service.
type Config struct {
	KafkaBrokers      string
	SchemaRegistryURL string
	Topic             string
	WorkerCount       int
	EventsPerSecond   int
}

// Load reads configuration from environment variables, falling back to
// local-development defaults when a variable is not set.
func Load() Config {
	return Config{
		KafkaBrokers:      getEnv("KAFKA_BROKERS", "localhost:9092"),
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Topic:             getEnv("USER_ACTIVITY_TOPIC", "user-activity.events"),
		WorkerCount:       getEnvInt("WORKER_COUNT", 4),
		EventsPerSecond:   getEnvInt("EVENTS_PER_SECOND", 50),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}
