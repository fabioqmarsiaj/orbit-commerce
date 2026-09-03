// Package schema owns the Go representation of the 3 user-activity.events
// Avro record types (ProductViewed, AddedToCart, SearchPerformed) and the
// machinery to register/resolve them against Schema Registry.
//
// The .avsc files under avro/ are copies of the canonical schemas defined
// in event-schemas/src/main/avro/useractivity/ (the Java Avro module every
// other service in this project generates its Avro classes from) - Go has
// no way to reference files outside its own module tree via go:embed, so
// this is a deliberate, documented duplication rather than an oversight.
// If those upstream .avsc files ever change, these 3 copies need to be
// updated to match by hand. See docs/decisions.md ("Phase 8 -
// ingestion-service") for the full reasoning.
package schema

import "time"

// EventType identifies which of the 3 user-activity.events Avro record
// types a given payload is - used both to pick the right cached
// (schema ID, parsed schema) pair in Registrar and to select which struct
// to encode.
type EventType string

const (
	ProductViewedType   EventType = "ProductViewed"
	AddedToCartType     EventType = "AddedToCart"
	SearchPerformedType EventType = "SearchPerformed"
)

// ProductViewed mirrors avro/ProductViewed.avsc field-for-field. Struct
// tags use hamba/avro's `avro:"..."` convention; occurredAt's
// long/timestamp-millis logical type maps automatically to time.Time (see
// hamba/avro's type-conversion table), no custom codec needed.
type ProductViewed struct {
	EventID    string    `avro:"eventId"`
	SessionID  string    `avro:"sessionId"`
	ProductID  string    `avro:"productId"`
	OccurredAt time.Time `avro:"occurredAt"`
}

// AddedToCart mirrors avro/AddedToCart.avsc field-for-field.
type AddedToCart struct {
	EventID    string    `avro:"eventId"`
	SessionID  string    `avro:"sessionId"`
	ProductID  string    `avro:"productId"`
	Quantity   int       `avro:"quantity"`
	OccurredAt time.Time `avro:"occurredAt"`
}

// SearchPerformed mirrors avro/SearchPerformed.avsc field-for-field.
type SearchPerformed struct {
	EventID    string    `avro:"eventId"`
	SessionID  string    `avro:"sessionId"`
	Query      string    `avro:"query"`
	OccurredAt time.Time `avro:"occurredAt"`
}

// Definition pairs one event type with the Schema Registry subject it must
// be registered under and the raw Avro schema JSON that defines it.
type Definition struct {
	EventType  EventType
	Subject    string
	SchemaJSON string
}

// definitions is the single source of truth for "which event types exist,
// what Schema Registry subject does each belong to, and what's their raw
// schema". Subjects use the record's fully-qualified name
// (namespace + "." + name), matching RecordNameStrategy - the same subject
// naming strategy every Java producer in this project already uses for
// multi-type topics (see docs/decisions.md, "Phase 3 - inventory-service",
// "Multi-type Kafka topics break Schema Registry compatibility checks
// under the default subject strategy"). user-activity.events is exactly
// that kind of topic: it carries 3 structurally unrelated record types.
var definitions = []Definition{
	{
		EventType:  ProductViewedType,
		Subject:    "com.fabioqmarsiaj.events.useractivity.ProductViewed",
		SchemaJSON: productViewedSchemaJSON,
	},
	{
		EventType:  AddedToCartType,
		Subject:    "com.fabioqmarsiaj.events.useractivity.AddedToCart",
		SchemaJSON: addedToCartSchemaJSON,
	},
	{
		EventType:  SearchPerformedType,
		Subject:    "com.fabioqmarsiaj.events.useractivity.SearchPerformed",
		SchemaJSON: searchPerformedSchemaJSON,
	},
}

// Definitions returns a copy of every known event type's Schema Registry
// subject + raw schema JSON - exported so tests (and any future caller
// that needs the raw schema without a live Schema Registry) don't need
// their own copy of these constants.
func Definitions() []Definition {
	out := make([]Definition, len(definitions))
	copy(out, definitions)
	return out
}
