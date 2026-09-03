package producer

import (
	"testing"
	"time"

	"github.com/hamba/avro/v2"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
)

// TestEncodeConfluentWire_RoundTrip verifies the wire format
// EncodeConfluentWire produces can be decoded back into an equivalent
// value - exercising the magic byte + schema ID + Avro binary framing
// without needing a live Schema Registry (schema.Definitions() gives us
// the raw schema JSON directly, and we assign a made-up schema ID
// ourselves, the same shape Registrar.Get would return after a real
// RegisterAll call).
func TestEncodeConfluentWire_RoundTrip(t *testing.T) {
	for _, def := range schema.Definitions() {
		t.Run(string(def.EventType), func(t *testing.T) {
			parsed, err := avro.Parse(def.SchemaJSON)
			if err != nil {
				t.Fatalf("avro.Parse(%s): %v", def.EventType, err)
			}
			reg := schema.Registered{ID: 42, Schema: parsed}

			payload := samplePayload(t, def.EventType)

			wire, err := EncodeConfluentWire(reg, payload)
			if err != nil {
				t.Fatalf("EncodeConfluentWire: %v", err)
			}

			if got := len(wire); got < 5 {
				t.Fatalf("wire length = %d, want at least 5 (1 magic byte + 4 schema ID bytes)", got)
			}
			if wire[0] != magicByte {
				t.Errorf("wire[0] = %#x, want magic byte %#x", wire[0], magicByte)
			}
			gotID := int(wire[1])<<24 | int(wire[2])<<16 | int(wire[3])<<8 | int(wire[4])
			if gotID != reg.ID {
				t.Errorf("decoded schema ID = %d, want %d", gotID, reg.ID)
			}

			decoded := newZeroValue(t, def.EventType)
			if err := avro.Unmarshal(parsed, wire[5:], decoded); err != nil {
				t.Fatalf("avro.Unmarshal of the Avro body (wire[5:]): %v", err)
			}
			assertEqual(t, def.EventType, payload, decoded)
		})
	}
}

func samplePayload(t *testing.T, eventType schema.EventType) any {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	switch eventType {
	case schema.ProductViewedType:
		return schema.ProductViewed{EventID: "evt-1", SessionID: "sess-1", ProductID: "sku-1", OccurredAt: now}
	case schema.AddedToCartType:
		return schema.AddedToCart{EventID: "evt-2", SessionID: "sess-1", ProductID: "sku-2", Quantity: 3, OccurredAt: now}
	case schema.SearchPerformedType:
		return schema.SearchPerformed{EventID: "evt-3", SessionID: "sess-1", Query: "headphones", OccurredAt: now}
	default:
		t.Fatalf("unrecognized event type %s", eventType)
		return nil
	}
}

func newZeroValue(t *testing.T, eventType schema.EventType) any {
	t.Helper()
	switch eventType {
	case schema.ProductViewedType:
		return &schema.ProductViewed{}
	case schema.AddedToCartType:
		return &schema.AddedToCart{}
	case schema.SearchPerformedType:
		return &schema.SearchPerformed{}
	default:
		t.Fatalf("unrecognized event type %s", eventType)
		return nil
	}
}

func assertEqual(t *testing.T, eventType schema.EventType, want any, got any) {
	t.Helper()
	switch w := want.(type) {
	case schema.ProductViewed:
		g := got.(*schema.ProductViewed)
		if w != *g {
			t.Errorf("%s round-trip mismatch:\n want %+v\n got  %+v", eventType, w, *g)
		}
	case schema.AddedToCart:
		g := got.(*schema.AddedToCart)
		if w != *g {
			t.Errorf("%s round-trip mismatch:\n want %+v\n got  %+v", eventType, w, *g)
		}
	case schema.SearchPerformed:
		g := got.(*schema.SearchPerformed)
		if w != *g {
			t.Errorf("%s round-trip mismatch:\n want %+v\n got  %+v", eventType, w, *g)
		}
	default:
		t.Fatalf("unrecognized payload type %T", want)
	}
}
