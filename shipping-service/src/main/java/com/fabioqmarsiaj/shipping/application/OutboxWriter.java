package com.fabioqmarsiaj.shipping.application;

import com.fabioqmarsiaj.events.shipping.ShipmentCreated;
import com.fabioqmarsiaj.events.shipping.ShipmentFailed;
import com.fabioqmarsiaj.outbox.application.OutboxRecorder;
import com.fabioqmarsiaj.shipping.messaging.KafkaTopics;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Writes outbox rows for shipping-service's outbound integration events
 * ({@code ShipmentCreated}/{@code ShipmentFailed}).
 *
 * <p>The mechanical part (Avro-JSON encoding + building/saving the outbox
 * entity) lives in the shared {@link OutboxRecorder} (see
 * {@code outbox-support} module, and {@code docs/decisions.md} for the
 * extraction writeup). This class's own job is purely shipping-service
 * specific: building the right Avro event object from the data
 * {@link ShippingCommandService} has on hand after processing a
 * {@code CreateShipmentCommand} — same shape as inventory-service's and
 * payment-service's {@code OutboxWriter}.
 */
@Component
public class OutboxWriter {

    private final OutboxRecorder outboxRecorder;

    public OutboxWriter(OutboxRecorder outboxRecorder) {
        this.outboxRecorder = outboxRecorder;
    }

    public void writeShipmentCreated(UUID orderId, String shipmentId) {
        ShipmentCreated event = new ShipmentCreated(
                UUID.randomUUID().toString(), orderId.toString(), shipmentId, Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.SHIPPING_EVENTS, event);
    }

    public void writeShipmentFailed(UUID orderId, String reason) {
        ShipmentFailed event = new ShipmentFailed(
                UUID.randomUUID().toString(), orderId.toString(), reason, Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.SHIPPING_EVENTS, event);
    }
}
