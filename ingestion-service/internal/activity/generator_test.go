package activity

import (
	"testing"
	"time"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
)

// TestGenerateSession_AlwaysHasAtLeastOneProductViewed asserts the one
// invariant GenerateSession's doc comment promises unconditionally: every
// session views at least one product, regardless of how the search/cart
// coin flips land.
func TestGenerateSession_AlwaysHasAtLeastOneProductViewed(t *testing.T) {
	g := newSeededGenerator(1, 2)
	for i := range 200 {
		events := g.GenerateSession()
		viewCount := countByType(events, schema.ProductViewedType)
		if viewCount < minViewsPerSession {
			t.Fatalf("session %d: got %d ProductViewed events, want at least %d", i, viewCount, minViewsPerSession)
		}
	}
}

// TestGenerateSession_SameSessionIDThroughout asserts every event within
// one GenerateSession call shares the same sessionId - this is the
// "correlated session, not independent random events" property the whole
// package exists to provide (see generator.go's Generator doc comment).
func TestGenerateSession_SameSessionIDThroughout(t *testing.T) {
	g := newSeededGenerator(3, 4)
	for i := range 200 {
		events := g.GenerateSession()
		sessionID := sessionIDOf(t, events[0])
		for _, e := range events {
			if got := sessionIDOf(t, e); got != sessionID {
				t.Fatalf("session %d: event has sessionId %q, want %q (all events in one session must share a sessionId)", i, got, sessionID)
			}
		}
	}
}

// TestGenerateSession_AddedToCartReferencesAViewedProduct asserts the
// funnel coherence rule: whenever a session ends with AddedToCart, its
// productId must be one the same session actually viewed earlier - never
// a product plucked from the catalog independently.
func TestGenerateSession_AddedToCartReferencesAViewedProduct(t *testing.T) {
	g := newSeededGenerator(5, 6)
	sawAtLeastOneCart := false
	for i := range 500 {
		events := g.GenerateSession()
		viewed := make(map[string]bool)
		for _, e := range events {
			if pv, ok := e.Payload.(schema.ProductViewed); ok {
				viewed[pv.ProductID] = true
			}
		}
		for _, e := range events {
			if cart, ok := e.Payload.(schema.AddedToCart); ok {
				sawAtLeastOneCart = true
				if !viewed[cart.ProductID] {
					t.Fatalf("session %d: AddedToCart references productId %q, which this session never viewed (viewed: %v)", i, cart.ProductID, viewed)
				}
			}
		}
	}
	if !sawAtLeastOneCart {
		t.Fatal("expected at least one AddedToCart across 500 sessions to exercise the assertion above - addToCartProbability may be misconfigured")
	}
}

// TestGenerateSession_TimestampsStrictlyIncreaseWithinSession asserts
// occurredAt is monotonically increasing within a session, matching the
// causal ordering (search, then views, then maybe cart) the funnel
// simulates.
func TestGenerateSession_TimestampsStrictlyIncreaseWithinSession(t *testing.T) {
	g := newSeededGenerator(7, 8)
	for i := range 200 {
		events := g.GenerateSession()
		for j := 1; j < len(events); j++ {
			prev := occurredAtOf(t, events[j-1])
			cur := occurredAtOf(t, events[j])
			if !cur.After(prev) {
				t.Fatalf("session %d: event %d occurredAt (%v) is not after event %d occurredAt (%v)", i, j, cur, j-1, prev)
			}
		}
	}
}

// TestGenerateSession_TypeDistributionIsWeightedTowardProductViewed
// asserts the aggregate mix across many sessions roughly matches the
// documented funnel weighting (search ~40% of sessions, cart ~20% of
// sessions, product views dominant overall) rather than a uniform 1/3
// split across the 3 event types - this is what makes the Kafka Streams
// "top viewed products" analytics (Phase 7 Part B) actually produce
// interesting, funnel-shaped counts instead of flat noise.
func TestGenerateSession_TypeDistributionIsWeightedTowardProductViewed(t *testing.T) {
	g := newSeededGenerator(9, 10)
	const sessions = 2000
	var views, carts, searches int
	for range sessions {
		for _, e := range g.GenerateSession() {
			switch e.Type {
			case schema.ProductViewedType:
				views++
			case schema.AddedToCartType:
				carts++
			case schema.SearchPerformedType:
				searches++
			}
		}
	}

	total := views + carts + searches
	viewRatio := float64(views) / float64(total)
	if viewRatio < 0.55 {
		t.Errorf("ProductViewed ratio = %.2f, want >= 0.55 (views should dominate the mix)", viewRatio)
	}

	searchRate := float64(searches) / float64(sessions)
	if searchRate < searchProbability-0.05 || searchRate > searchProbability+0.05 {
		t.Errorf("search-per-session rate = %.2f, want close to %.2f (searchProbability)", searchRate, searchProbability)
	}

	cartRate := float64(carts) / float64(sessions)
	if cartRate < addToCartProbability-0.05 || cartRate > addToCartProbability+0.05 {
		t.Errorf("cart-per-session rate = %.2f, want close to %.2f (addToCartProbability)", cartRate, addToCartProbability)
	}
}

func countByType(events []Event, t schema.EventType) int {
	n := 0
	for _, e := range events {
		if e.Type == t {
			n++
		}
	}
	return n
}

func sessionIDOf(t *testing.T, e Event) string {
	t.Helper()
	switch p := e.Payload.(type) {
	case schema.ProductViewed:
		return p.SessionID
	case schema.AddedToCart:
		return p.SessionID
	case schema.SearchPerformed:
		return p.SessionID
	default:
		t.Fatalf("unexpected payload type %T", e.Payload)
		return ""
	}
}

func occurredAtOf(t *testing.T, e Event) time.Time {
	t.Helper()
	switch p := e.Payload.(type) {
	case schema.ProductViewed:
		return p.OccurredAt
	case schema.AddedToCart:
		return p.OccurredAt
	case schema.SearchPerformed:
		return p.OccurredAt
	default:
		t.Fatalf("unexpected payload type %T", e.Payload)
		return time.Time{}
	}
}
