package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the inventory-service confirms that stock has been
 * successfully reserved for this order's line items.
 */
public record StockReserved(
        UUID eventId,
        UUID orderId,
        Instant occurredAt
) implements OrderDomainEvent {
}
