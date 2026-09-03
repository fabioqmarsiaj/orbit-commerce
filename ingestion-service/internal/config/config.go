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
	HTTPPort          int
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
		// 8090: the next free port after the 5 Spring Boot services
		// (8082-8086), Kafka UI (8080), and Schema Registry (8081) - see
		// docs/decisions.md ("Phase 8 - ingestion-service") for the full
		// port allocation table.
		HTTPPort: getEnvInt("HTTP_PORT", 8090),
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
