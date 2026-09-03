// Package stats holds process-wide counters shared between the worker
// pool (which increments them as events are published) and the HTTP API
// (which reads them for GET /stats) - a plain struct of atomics rather
// than anything fancier, since this is the only cross-cutting shared
// state in the whole service.
package stats

import (
	"sync/atomic"
	"time"
)

// Counters tracks how many events of each type have been published
// successfully, plus how many publish attempts failed. Safe for
// concurrent use by any number of worker goroutines and HTTP handlers.
type Counters struct {
	startedAt       time.Time
	productViewed   atomic.Int64
	addedToCart     atomic.Int64
	searchPerformed atomic.Int64
	publishErrors   atomic.Int64
}

// New creates a Counters with its start time set to now - used by
// GET /stats to report uptime.
func New() *Counters {
	return &Counters{startedAt: time.Now()}
}

// IncProductViewed records one successfully published ProductViewed event.
func (c *Counters) IncProductViewed() { c.productViewed.Add(1) }

// IncAddedToCart records one successfully published AddedToCart event.
func (c *Counters) IncAddedToCart() { c.addedToCart.Add(1) }

// IncSearchPerformed records one successfully published SearchPerformed
// event.
func (c *Counters) IncSearchPerformed() { c.searchPerformed.Add(1) }

// IncPublishErrors records one failed publish attempt (any event type).
func (c *Counters) IncPublishErrors() { c.publishErrors.Add(1) }

// Snapshot is an immutable, JSON-serializable point-in-time view of
// Counters, returned by GET /stats.
type Snapshot struct {
	UptimeSeconds        float64 `json:"uptimeSeconds"`
	ProductViewedCount   int64   `json:"productViewedCount"`
	AddedToCartCount     int64   `json:"addedToCartCount"`
	SearchPerformedCount int64   `json:"searchPerformedCount"`
	TotalPublished       int64   `json:"totalPublished"`
	PublishErrors        int64   `json:"publishErrors"`
}

// Snapshot reads every counter's current value into a Snapshot.
func (c *Counters) Snapshot() Snapshot {
	views := c.productViewed.Load()
	carts := c.addedToCart.Load()
	searches := c.searchPerformed.Load()
	return Snapshot{
		UptimeSeconds:        time.Since(c.startedAt).Seconds(),
		ProductViewedCount:   views,
		AddedToCartCount:     carts,
		SearchPerformedCount: searches,
		TotalPublished:       views + carts + searches,
		PublishErrors:        c.publishErrors.Load(),
	}
}
