package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the payment-service declines payment for this order. This
 * starts a compensation path: the previously reserved stock must be
 * released before the order transitions to {@code CANCELLED}.
 */
public record PaymentDeclined(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String reason
) implements OrderDomainEvent {
}
