package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the shipping-service confirms that a shipment was created
 * for this order. This is the final, happy-path event before the order is
 * marked {@code COMPLETED}.
 */
public record ShipmentCreated(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String shipmentId
) implements OrderDomainEvent {
}
