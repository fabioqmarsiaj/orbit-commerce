package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Recorded when the payment-service confirms that payment for this order
 * was successfully processed.
 */
public record PaymentApproved(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String paymentId
) implements OrderDomainEvent {
}
