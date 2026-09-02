package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Terminal event: the order could not be fulfilled after payment had
 * already been taken (compensation triggered by {@link ShipmentFailed}).
 * Unlike {@link OrderCancelled}, this represents a failure late in the
 * Saga, after money changed hands (which was then refunded).
 */
public record OrderFailed(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String reason
) implements OrderDomainEvent {
}
