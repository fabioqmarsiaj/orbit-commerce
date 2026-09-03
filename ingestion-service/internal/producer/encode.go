// Package producer encodes activity.Event payloads into the Confluent
// wire format and publishes them to user-activity.events via kafka-go.
package producer

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/schema"
)

// magicByte is the single fixed byte every Confluent-wire-format-encoded
// message starts with, identifying the format version. See
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
// - the same wire format order-service/inventory-service/etc.'s Java
// KafkaAvroSerializer produces, and what query-service's
// KafkaAvroDeserializer (Part A) and GenericAvroSerde (Part B) already
// expect to read on the consumer side.
const magicByte = 0x0

// EncodeConfluentWire serializes payload (one of schema.ProductViewed,
// schema.AddedToCart, or schema.SearchPerformed) as Avro binary, and
// wraps it in the Confluent wire format: magic byte (1) + schema ID
// (big-endian uint32, 4 bytes) + Avro binary payload. reg must be the
// Registered pair schema.Registrar.Get returned for this payload's event
// type - encoding with the wrong schema ID would silently corrupt the
// message for any consumer that resolves the schema by ID (which is every
// consumer in this project - see docs/decisions.md, "Phase 3 -
// inventory-service").
func EncodeConfluentWire(reg schema.Registered, payload any) ([]byte, error) {
	body, err := avro.Marshal(reg.Schema, payload)
	if err != nil {
		return nil, fmt.Errorf("producer: avro-encoding %T: %w", payload, err)
	}

	var buf bytes.Buffer
	buf.WriteByte(magicByte)
	buf.WriteByte(byte(reg.ID >> 24))
	buf.WriteByte(byte(reg.ID >> 16))
	buf.WriteByte(byte(reg.ID >> 8))
	buf.WriteByte(byte(reg.ID))
	buf.Write(body)
	return buf.Bytes(), nil
}
