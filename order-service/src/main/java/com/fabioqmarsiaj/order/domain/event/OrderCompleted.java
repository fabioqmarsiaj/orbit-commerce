package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Terminal event: the order was successfully fulfilled end-to-end (stock
 * reserved, payment approved, shipment created).
 */
public record OrderCompleted(
        UUID eventId,
        UUID orderId,
        Instant occurredAt
) implements OrderDomainEvent {
}
