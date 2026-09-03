package producer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/activity"
	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
)

// keyOf extracts the sessionId from an activity.Event's payload, used as
// the Kafka message key. Keying by sessionId (rather than, say, a random
// key or productId) means every event belonging to the same simulated
// session lands on the same partition and keeps its relative order -
// mirroring how every other topic in this project is keyed by the
// "aggregate" the events are about (orderId for order.events, productId
// for inventory.events, etc. - see docs/decisions.md across every prior
// phase).
func keyOf(e activity.Event) (string, error) {
	switch p := e.Payload.(type) {
	case schema.ProductViewed:
		return p.SessionID, nil
	case schema.AddedToCart:
		return p.SessionID, nil
	case schema.SearchPerformed:
		return p.SessionID, nil
	default:
		return "", fmt.Errorf("producer: unrecognized payload type %T", e.Payload)
	}
}

// Publisher publishes activity.Event values to user-activity.events,
// encoding each one in the Confluent wire format using schemas already
// resolved by a schema.Registrar.
type Publisher struct {
	writer *kafka.Writer
	reg    *schema.Registrar
}

// NewPublisher creates a Publisher writing to topic on brokers, using reg
// to resolve each event type's (schema ID, parsed schema) pair. reg must
// already have had RegisterAll called on it.
//
// RequiredAcks is left at kafka-go's default (RequireAll, i.e. acks=all)
// deliberately: this project's Java producers all wait for Kafka to
// actually acknowledge a send before considering it done (see
// docs/decisions.md, "Outbox publish is synchronous (.join()), by design,
// for at-least-once delivery") - matching that here, even though
// ingestion-service itself has no outbox/retry mechanism of its own (see
// Publish's doc comment for why that asymmetry is intentional).
func NewPublisher(brokers []string, topic string, reg *schema.Registrar) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{}, // partitions by key (sessionId), same session -> same partition
		},
		reg: reg,
	}
}

// Publish encodes and sends a single activity.Event.
//
// Unlike every other Kafka publisher in this project, there is
// deliberately no outbox/retry here: ingestion-service produces
// best-effort simulated traffic with no participation in the Saga and no
// domain state of its own to keep consistent with an "intent to publish"
// - a failed publish is simply logged and counted (see internal/worker),
// not retried. This is a real, intentional asymmetry with the rest of the
// project, not an oversight; see docs/decisions.md ("Phase 8 -
// ingestion-service") for the full reasoning.
func (p *Publisher) Publish(ctx context.Context, event activity.Event) error {
	reg, ok := p.reg.Get(event.Type)
	if !ok {
		return fmt.Errorf("producer: no registered schema for event type %s (was RegisterAll called?)", event.Type)
	}

	value, err := EncodeConfluentWire(reg, event.Payload)
	if err != nil {
		return err
	}

	key, err := keyOf(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

// Close flushes and closes the underlying Kafka writer.
func (p *Publisher) Close() error {
	return p.writer.Close()
}
