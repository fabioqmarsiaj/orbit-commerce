package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the shipping-service fails to create a shipment for this
 * order. This starts the final compensation path: both the payment must be
 * refunded and the reserved stock released before the order transitions to
 * {@code FAILED}.
 */
public record ShipmentFailed(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String reason
) implements OrderDomainEvent {
}
