// Package activity generates simulated user-activity.events traffic: a
// weighted, session-correlated stream of ProductViewed/AddedToCart/
// SearchPerformed events, standing in for real browsing activity until
// a real storefront (out of scope for this project) exists to produce it.
package activity

// productCatalog is a fixed, self-contained list of fake product IDs.
// Deliberately NOT sourced from inventory-service: that service's
// productIds are created ad hoc via POST /stock during manual testing
// (no fixed list, no listing endpoint), and user-activity.events'
// analytics (Phase 7 Part B's "top viewed products" Kafka Streams
// topology) only cares about counting views per productId, not about
// those products actually existing in a real Saga - coupling this
// generator to inventory-service's runtime state would add a dependency
// this simulation doesn't need. See docs/decisions.md ("Phase 8 -
// ingestion-service") for the full reasoning.
var productCatalog = []string{
	"sku-1", "sku-2", "sku-3", "sku-4", "sku-5",
	"sku-6", "sku-7", "sku-8", "sku-9", "sku-10",
	"sku-11", "sku-12", "sku-13", "sku-14", "sku-15",
	"sku-16", "sku-17", "sku-18", "sku-19", "sku-20",
	"sku-21", "sku-22", "sku-23", "sku-24", "sku-25",
}

// searchTerms is a fixed list of fake search queries used for
// SearchPerformed events - just needs to look like plausible e-commerce
// search input, not correspond to anything real.
var searchTerms = []string{
	"wireless headphones",
	"running shoes",
	"coffee maker",
	"laptop stand",
	"yoga mat",
	"desk lamp",
	"water bottle",
	"backpack",
	"bluetooth speaker",
	"phone case",
	"office chair",
	"winter jacket",
}
