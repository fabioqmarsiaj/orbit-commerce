package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.OrderEventTranslator;
import com.fabioqmarsiaj.order.persistence.OutboxEntity;
import com.fabioqmarsiaj.order.persistence.OutboxRepository;
import com.fabioqmarsiaj.order.persistence.OutboxStatus;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Turns publishable domain events into {@link OutboxEntity} rows.
 *
 * <p>The outbox payload stores the JSON serialization of the Avro-mapped
 * object (produced via {@link OrderEventTranslator#toAvro}), NOT the raw
 * domain event — this is what lets {@code OutboxPublisher} later
 * reconstruct the exact Avro object to publish without needing access to
 * the domain model at all. {@code eventType} stores the Avro class's
 * simple name (e.g. {@code "OrderCreated"} — note this happens to collide
 * with the domain event's own simple name, but they're serialized/read by
 * two entirely different mappers: {@code OrderEventMapper} for the event
 * store, this class + {@code OutboxPublisher} for the outbox).
 *
 * <p><b>Important:</b> do NOT use a generic Jackson {@code ObjectMapper} to
 * serialize the Avro {@link SpecificRecord} here. Avro-generated classes
 * expose a {@code getSchema()} bean-style getter returning
 * {@code org.apache.avro.Schema}, a complex object Jackson's default bean
 * introspection cannot cleanly serialize/deserialize. Instead, use Avro's
 * own JSON encoding support: {@link org.apache.avro.io.EncoderFactory#jsonEncoder}
 * with a {@link org.apache.avro.specific.SpecificDatumWriter}, writing into
 * a {@link java.io.ByteArrayOutputStream}. This is the same encoding
 * {@code OutboxPublisher} will later reverse with
 * {@link org.apache.avro.io.DecoderFactory#jsonDecoder} +
 * {@link org.apache.avro.specific.SpecificDatumReader}.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final OrderEventTranslator translator;

    public OutboxWriter(OutboxRepository repository, OrderEventTranslator translator) {
        this.repository = repository;
        this.translator = translator;
    }

    /**
     * Filters {@code events} down to the publishable ones and writes one
     * {@link OutboxEntity} row per event, all with {@link OutboxStatus#PENDING}.
     * Must be called within the same transaction as
     * {@code OrderEventStore#append} for the Outbox pattern's atomicity
     * guarantee to hold.
     */
    public void writeAll(UUID orderId, List<OrderDomainEvent> events) {
        // TODO: for each event in events:
        //  1. Skip it if !translator.isPublishable(event).
        //  2. SpecificRecord avroRecord = translator.toAvro(event);
        //  3. Serialize avroRecord to an Avro-JSON string (see class javadoc
        //     above for the exact API to use) — consider extracting this
        //     into a small private helper, e.g.
        //     `private String toAvroJson(SpecificRecord record)`.
        //  4. Build a new OutboxEntity(UUID.randomUUID(), orderId,
        //     KafkaTopics.ORDER_EVENTS, avroRecord.getClass().getSimpleName(),
        //     json, OutboxStatus.PENDING, Instant.now()) and repository.save(...) it.
        throw new UnsupportedOperationException("not implemented yet");
    }
}
