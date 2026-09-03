package schema

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/registry"
)

// Registered pairs a parsed Avro schema with the numeric ID Schema
// Registry assigned it - both are required to encode a message in the
// Confluent wire format (magic byte + 4-byte schema ID + Avro binary; see
// internal/producer).
type Registered struct {
	ID     int
	Schema avro.Schema
}

// Registrar resolves and caches a (schema ID, parsed schema) pair for
// every user-activity.events record type - it talks to Schema Registry
// exactly once per type, at startup, rather than on every publish.
type Registrar struct {
	client *registry.Client
	cache  map[EventType]Registered
}

// NewRegistrar creates a Registrar backed by the Schema Registry at
// baseURL. It does not perform any network I/O itself - call RegisterAll
// to actually resolve and cache every schema.
func NewRegistrar(baseURL string) (*Registrar, error) {
	client, err := registry.NewClient(baseURL)
	if err != nil {
		return nil, fmt.Errorf("schema: creating registry client: %w", err)
	}
	return &Registrar{client: client, cache: make(map[EventType]Registered)}, nil
}

// RegisterAll registers every user-activity.events record type's schema
// with Schema Registry (one subject per record's fully-qualified name,
// per RecordNameStrategy - see Definitions' doc comment) and caches the
// resulting (schema ID, parsed schema) pair for Get.
//
// CreateSchema is idempotent: registering an already-registered, unchanged
// schema just returns its existing ID rather than erroring or creating a
// duplicate - the same idempotent-by-design shape as
// infra/kafka/init-topics.sh's own `--if-not-exists` topic creation. That
// makes it safe to call this on every process start, with no separate
// "does this already exist" check needed first.
func (r *Registrar) RegisterAll(ctx context.Context) error {
	for _, def := range definitions {
		id, parsed, err := r.client.CreateSchema(ctx, def.Subject, def.SchemaJSON)
		if err != nil {
			return fmt.Errorf("schema: registering subject %s: %w", def.Subject, err)
		}
		r.cache[def.EventType] = Registered{ID: id, Schema: parsed}
	}
	return nil
}

// Get returns the cached (schema ID, parsed schema) pair for the given
// event type. ok is false if RegisterAll hasn't been called yet, or if
// eventType isn't one of the 3 known user-activity.events record types.
func (r *Registrar) Get(eventType EventType) (Registered, bool) {
	reg, ok := r.cache[eventType]
	return reg, ok
}
