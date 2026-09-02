package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the inventory-service could not reserve stock for this
 * order (e.g. insufficient quantity). This starts the compensation path:
 * the order will transition straight to {@code CANCELLED}.
 */
public record StockRejected(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String reason
) implements OrderDomainEvent {
}
