// Command ingestion-service is a high-throughput producer that simulates
// user activity events (product views, add-to-cart, search) and publishes
// them to Kafka's user-activity.events topic. See docs/decisions.md
// ("Phase 8 - ingestion-service") for the full design writeup.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/config"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/httpapi"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/producer"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/stats"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/worker"
)

func main() {
	cfg := config.Load()
	log.Printf("orbit-commerce ingestion-service starting (workers=%d, eventsPerSecond=%d, topic=%s, httpPort=%d)",
		cfg.WorkerCount, cfg.EventsPerSecond, cfg.Topic, cfg.HTTPPort)

	// Cancel ctx on SIGINT/SIGTERM (Ctrl+C, or `docker stop`/graceful
	// process managers) - every long-running piece below (the worker
	// pool's publish loop, the rate limiter's Wait calls) observes this
	// same ctx and unwinds cleanly instead of being killed mid-publish.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registrar, err := schema.NewRegistrar(cfg.SchemaRegistryURL)
	if err != nil {
		log.Fatalf("ingestion-service: %v", err)
	}
	// Registering (or confirming already-registered) all 3 schemas at
	// startup, before the worker pool ever tries to publish, means a
	// Schema Registry that's still starting up (see docker-compose.yml's
	// healthcheck-gated startup ordering) fails loudly and immediately
	// here, rather than surfacing as a confusing per-event error deep
	// inside the worker pool later.
	registerCtx, cancelRegister := context.WithTimeout(ctx, 30*time.Second)
	err = registrar.RegisterAll(registerCtx)
	cancelRegister()
	if err != nil {
		log.Fatalf("ingestion-service: registering Avro schemas: %v", err)
	}
	log.Print("ingestion-service: registered all 3 user-activity.events schemas with Schema Registry")

	pub := producer.NewPublisher([]string{cfg.KafkaBrokers}, cfg.Topic, registrar)
	defer func() {
		if err := pub.Close(); err != nil {
			log.Printf("ingestion-service: error closing Kafka writer: %v", err)
		}
	}()

	counters := stats.New()
	pool := worker.NewPool(cfg.WorkerCount, cfg.EventsPerSecond, pub, counters)

	httpServer := httpapi.NewServer(fmt.Sprintf(":%d", cfg.HTTPPort), counters)
	go func() {
		log.Printf("ingestion-service: HTTP server listening on %s (GET /health, GET /stats)", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("ingestion-service: HTTP server error: %v", err)
		}
	}()

	// Pool.Run blocks until ctx is canceled (SIGINT/SIGTERM), then every
	// worker goroutine finishes its current publish and returns.
	pool.Run(ctx)

	log.Print("ingestion-service: shutdown signal received, worker pool stopped, shutting down HTTP server")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("ingestion-service: error shutting down HTTP server: %v", err)
	}
}
