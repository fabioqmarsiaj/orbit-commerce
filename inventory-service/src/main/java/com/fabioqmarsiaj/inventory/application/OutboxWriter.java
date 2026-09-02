package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.inventory.messaging.KafkaTopics;
import com.fabioqmarsiaj.inventory.persistence.OutboxEntity;
import com.fabioqmarsiaj.inventory.persistence.OutboxRepository;
import com.fabioqmarsiaj.inventory.persistence.OutboxStatus;
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
import java.util.UUID;

/**
 * Writes {@link OutboxEntity} rows for inventory-service's outbound
 * integration events ({@code StockReserved}/{@code StockRejected}/
 * {@code StockReleased}).
 *
 * <p>Unlike {@code order-service}'s {@code OutboxWriter} — which translates
 * a list of already-raised domain events pulled off an event-sourced
 * aggregate — inventory-service has no aggregate/event-sourcing layer.
 * Each write method here is called directly by
 * {@link InventoryCommandService} with just the data needed to build one
 * specific Avro event, right after {@link StockService} reports the
 * outcome of a reservation/release attempt.
 *
 * <p>Same Avro-JSON encoding approach as {@code order-service}'s
 * {@code OutboxWriter} (Avro's own {@link EncoderFactory#jsonEncoder}
 * rather than plain Jackson — see that class's Javadoc, and
 * {@code docs/decisions.md}, for why: Avro {@link SpecificRecord} classes
 * expose a bean-style {@code getSchema()} getter that generic Jackson bean
 * introspection can't handle).
 */
@Component
public class OutboxWriter {

    private final OutboxRepository repository;

    public OutboxWriter(OutboxRepository repository) {
        this.repository = repository;
    }

    public void writeStockReserved(UUID orderId) {
        StockReserved event = new StockReserved(
                UUID.randomUUID().toString(), orderId.toString(), Instant.now());
        write(orderId, event);
    }

    public void writeStockRejected(UUID orderId, String reason) {
        StockRejected event = new StockRejected(
                UUID.randomUUID().toString(), orderId.toString(), reason, Instant.now());
        write(orderId, event);
    }

    public void writeStockReleased(UUID orderId) {
        StockReleased event = new StockReleased(
                UUID.randomUUID().toString(), orderId.toString(), Instant.now());
        write(orderId, event);
    }

    private void write(UUID orderId, SpecificRecord avroRecord) {
        String json = toAvroJson(avroRecord);
        OutboxEntity outboxEntity = new OutboxEntity(
                UUID.randomUUID(),
                orderId,
                KafkaTopics.INVENTORY_EVENTS,
                avroRecord.getClass().getSimpleName(),
                json,
                OutboxStatus.PENDING,
                Instant.now()
        );
        repository.save(outboxEntity);
    }

    /**
     * Encodes an Avro {@link SpecificRecord} as a JSON string using Avro's
     * own schema-aware JSON codec (see class javadoc for why this can't be
     * plain Jackson).
     */
    private String toAvroJson(SpecificRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            @SuppressWarnings("unchecked")
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
