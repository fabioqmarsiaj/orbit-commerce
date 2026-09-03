// Package worker runs a pool of goroutines that continuously generate
// simulated user-activity.events sessions and publish them, respecting a
// global events-per-second rate limit.
package worker

import (
	"context"
	"log"

	"golang.org/x/time/rate"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/activity"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/producer"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/stats"
)

// Pool runs workerCount goroutines, each continuously generating sessions
// (via activity.Generator) and publishing their events (via
// producer.Publisher), until ctx is canceled. A single rate.Limiter is
// shared across every worker so eventsPerSecond is an aggregate limit
// across the whole pool, not a per-worker one - matching how
// config.Config.EventsPerSecond is documented ("simulate high volume"
// overall, per TASKS.md Phase 8, not per goroutine).
type Pool struct {
	workerCount int
	limiter     *rate.Limiter
	publisher   *producer.Publisher
	counters    *stats.Counters
}

// NewPool creates a Pool. eventsPerSecond configures the shared rate
// limiter's steady-state rate; its burst is set equal to the rate itself
// (i.e. up to one full second's worth of events can fire back-to-back
// after an idle period) - a reasonable default for a simulation with no
// external SLA to honor precisely.
func NewPool(workerCount, eventsPerSecond int, publisher *producer.Publisher, counters *stats.Counters) *Pool {
	return &Pool{
		workerCount: workerCount,
		limiter:     rate.NewLimiter(rate.Limit(eventsPerSecond), eventsPerSecond),
		publisher:   publisher,
		counters:    counters,
	}
}

// Run starts workerCount goroutines and blocks until ctx is canceled, at
// which point every worker finishes its current in-flight publish (if
// any) and returns.
func (p *Pool) Run(ctx context.Context) {
	done := make(chan struct{})
	for range p.workerCount {
		go func() {
			p.runOne(ctx)
			done <- struct{}{}
		}()
	}
	for range p.workerCount {
		<-done
	}
}

// runOne is a single worker goroutine's loop: generate a session, publish
// each of its events (one rate-limiter token per event), repeat until ctx
// is canceled.
func (p *Pool) runOne(ctx context.Context) {
	gen := activity.NewGenerator()
	for {
		for _, event := range gen.GenerateSession() {
			if err := p.limiter.Wait(ctx); err != nil {
				// ctx was canceled while waiting for a rate-limit token -
				// time to shut down, not a real error.
				return
			}
			if err := p.publisher.Publish(ctx, event); err != nil {
				p.counters.IncPublishErrors()
				log.Printf("ingestion-service: failed to publish %s event: %v", event.Type, err)
				continue
			}
			recordSuccess(p.counters, event.Type)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func recordSuccess(counters *stats.Counters, eventType schema.EventType) {
	switch eventType {
	case schema.ProductViewedType:
		counters.IncProductViewed()
	case schema.AddedToCartType:
		counters.IncAddedToCart()
	case schema.SearchPerformedType:
		counters.IncSearchPerformed()
	}
}
