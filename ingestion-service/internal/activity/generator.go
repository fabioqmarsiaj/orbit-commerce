package activity

import (
	"math/rand/v2"
	"time"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
)

// searchProbability is the chance a session opens with a SearchPerformed
// event before browsing any products - not every session starts from a
// search (some arrive via a direct link, a recommendation, etc.).
const searchProbability = 0.40

// addToCartProbability is the chance a session ends with an AddedToCart
// event for one of the products it viewed - most sessions just browse
// and leave, matching typical e-commerce funnel drop-off.
const addToCartProbability = 0.20

// minViewsPerSession / maxViewsPerSession bound how many ProductViewed
// events a single session generates - every session views at least one
// product (that's the point of the session), rarely more than three.
const (
	minViewsPerSession = 1
	maxViewsPerSession = 3
)

// Event pairs a schema.EventType with its concrete payload struct
// (schema.ProductViewed, schema.AddedToCart, or schema.SearchPerformed) -
// producer.Publisher switches on Type to pick the right cached schema and
// encoder.
type Event struct {
	Type    schema.EventType
	Payload any
}

// Generator produces one simulated user session's worth of
// user-activity.events at a time via GenerateSession. A session is a
// short, causally-ordered funnel - not a set of independent random
// events - modeling how a real shopper actually behaves: optionally
// search, view one or more products, then maybe add one of the viewed
// products to the cart. Every event in a session shares the same
// sessionId, and AddedToCart (when it happens) always references a
// productId the same session already viewed - a coherence rule a purely
// per-event-independent random generator couldn't guarantee.
type Generator struct {
	rng *rand.Rand
}

// NewGenerator creates a Generator seeded from a cryptographically
// unpredictable source - fine here since this is simulated traffic, not
// anything security-sensitive.
func NewGenerator() *Generator {
	return &Generator{rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())>>1))} //nolint:gosec
}

// newSeededGenerator creates a Generator with a fixed seed - used only by
// tests, so distribution assertions are deterministic instead of flaky.
func newSeededGenerator(seed1, seed2 uint64) *Generator {
	return &Generator{rng: rand.New(rand.NewPCG(seed1, seed2))}
}

// GenerateSession produces one session's worth of events, in the order
// they logically occurred (and with strictly increasing occurredAt
// timestamps to match). The returned slice always has at least one
// ProductViewed event.
func (g *Generator) GenerateSession() []Event {
	sessionID := newID()
	now := time.Now().UTC()
	step := 0 // ticks a few seconds forward per event, so occurredAt strictly increases within the session

	nextTimestamp := func() time.Time {
		t := now.Add(time.Duration(step) * 7 * time.Second)
		step++
		return t
	}

	var events []Event

	if g.rng.Float64() < searchProbability {
		events = append(events, Event{
			Type: schema.SearchPerformedType,
			Payload: schema.SearchPerformed{
				EventID:    newID(),
				SessionID:  sessionID,
				Query:      pick(g.rng, searchTerms),
				OccurredAt: nextTimestamp(),
			},
		})
	}

	viewCount := minViewsPerSession + g.rng.IntN(maxViewsPerSession-minViewsPerSession+1)
	viewedProducts := make([]string, 0, viewCount)
	for range viewCount {
		productID := pick(g.rng, productCatalog)
		viewedProducts = append(viewedProducts, productID)
		events = append(events, Event{
			Type: schema.ProductViewedType,
			Payload: schema.ProductViewed{
				EventID:    newID(),
				SessionID:  sessionID,
				ProductID:  productID,
				OccurredAt: nextTimestamp(),
			},
		})
	}

	if g.rng.Float64() < addToCartProbability {
		productID := viewedProducts[g.rng.IntN(len(viewedProducts))]
		events = append(events, Event{
			Type: schema.AddedToCartType,
			Payload: schema.AddedToCart{
				EventID:    newID(),
				SessionID:  sessionID,
				ProductID:  productID,
				Quantity:   1 + g.rng.IntN(3),
				OccurredAt: nextTimestamp(),
			},
		})
	}

	return events
}

// pick returns a uniformly random element of items.
func pick(rng *rand.Rand, items []string) string {
	return items[rng.IntN(len(items))]
}
