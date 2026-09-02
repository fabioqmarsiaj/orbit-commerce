package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Terminal event: the order was cancelled before payment/shipment took
 * place (compensation triggered by {@link StockRejected} or
 * {@link PaymentDeclined}).
 */
public record OrderCancelled(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String reason
) implements OrderDomainEvent {
}
