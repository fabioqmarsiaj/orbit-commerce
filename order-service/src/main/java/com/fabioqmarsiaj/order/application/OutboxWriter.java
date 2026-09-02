package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.OrderEventTranslator;
import com.fabioqmarsiaj.order.persistence.OutboxEntity;
import com.fabioqmarsiaj.order.persistence.OutboxRepository;
import com.fabioqmarsiaj.order.persistence.OutboxStatus;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
 * introspection cannot cleanly serialize/deserialize. Instead, we use
 * Avro's own JSON encoding support: {@link EncoderFactory#jsonEncoder} with
 * a {@link SpecificDatumWriter}, writing into a
 * {@link ByteArrayOutputStream}. This is the same encoding
 * {@code OutboxPublisher} reverses with {@code DecoderFactory#jsonDecoder}
 * + {@code SpecificDatumReader}.
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
        for (OrderDomainEvent event : events) {
            if (!translator.isPublishable(event)) {
                continue;
            }

            SpecificRecord avroRecord = translator.toAvro(event);
            String json = toAvroJson(avroRecord);

            OutboxEntity outboxEntity = new OutboxEntity(
                    UUID.randomUUID(),
                    orderId,
                    KafkaTopics.ORDER_EVENTS,
                    avroRecord.getClass().getSimpleName(),
                    json,
                    OutboxStatus.PENDING,
                    Instant.now()
            );
            repository.save(outboxEntity);
        }
    }

    /**
     * Encodes an Avro {@link SpecificRecord} as a JSON string using Avro's
     * own schema-aware JSON codec (see class javadoc for why this can't be
     * plain Jackson).
     */
    @SuppressWarnings("unchecked")
    private String toAvroJson(SpecificRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
            writer.write(record, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to Avro-JSON encode " + record.getClass().getSimpleName(), e);
        }
    }
}
