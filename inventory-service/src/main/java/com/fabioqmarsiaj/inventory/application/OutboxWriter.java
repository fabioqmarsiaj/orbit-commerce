package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.inventory.messaging.KafkaTopics;
import com.fabioqmarsiaj.outbox.application.OutboxRecorder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Writes outbox rows for inventory-service's outbound integration events
 * ({@code StockReserved}/{@code StockRejected}/{@code StockReleased}).
 *
 * <p>The mechanical part (Avro-JSON encoding + building/saving the outbox
 * entity) lives in the shared {@link OutboxRecorder} (see
 * {@code outbox-support} module, and {@code docs/decisions.md} for the
 * extraction writeup). This class's own job is purely inventory-service
 * specific: building the right Avro event object from the data
 * {@link InventoryCommandService} has on hand after a
 * reservation/release attempt — inventory-service has no aggregate/
 * event-sourcing layer to translate from, unlike order-service's
 * {@code OutboxWriter}.
 */
@Component
public class OutboxWriter {

    private final OutboxRecorder outboxRecorder;

    public OutboxWriter(OutboxRecorder outboxRecorder) {
        this.outboxRecorder = outboxRecorder;
    }

    public void writeStockReserved(UUID orderId) {
        StockReserved event = new StockReserved(
                UUID.randomUUID().toString(), orderId.toString(), Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, event);
    }

    public void writeStockRejected(UUID orderId, String reason) {
        StockRejected event = new StockRejected(
                UUID.randomUUID().toString(), orderId.toString(), reason, Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, event);
    }

    public void writeStockReleased(UUID orderId) {
        StockReleased event = new StockReleased(
                UUID.randomUUID().toString(), orderId.toString(), Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, event);
    }
}
