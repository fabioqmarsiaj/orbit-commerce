package com.fabioqmarsiaj.order.domain.event;

import com.fabioqmarsiaj.order.domain.OrderLineItem;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Recorded when a new order is first placed by a customer. This is always
 * the first event in an order's event stream.
 */
public record OrderCreated(
        UUID eventId,
        UUID orderId,
        Instant occurredAt,
        String customerId,
        List<OrderLineItem> items,
        long totalAmountCents
) implements OrderDomainEvent {
}
