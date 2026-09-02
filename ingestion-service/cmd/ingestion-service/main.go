// Command ingestion-service is a high-throughput producer that simulates
// user activity events (product views, add-to-cart, search) and publishes
// them to Kafka's user-activity.events topic.
//
// This is a Phase 0 scaffold; the full worker-pool based producer and Kafka
// wiring will be implemented in Phase 8 (see TASKS.md).
package main

import (
	"log"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/config"
)

func main() {
	cfg := config.Load()
	log.Printf("orbit-commerce ingestion-service starting (workers=%d, eventsPerSecond=%d)",
		cfg.WorkerCount, cfg.EventsPerSecond)
}
