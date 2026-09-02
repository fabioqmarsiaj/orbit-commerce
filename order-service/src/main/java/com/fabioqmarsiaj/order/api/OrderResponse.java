package com.fabioqmarsiaj.order.api;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.domain.OrderStatus;

import java.util.List;
import java.util.UUID;

/**
 * Response body for {@code GET /orders/{id}}. A read-facing projection of
 * the {@link Order} aggregate — deliberately a separate type from the
 * domain model so the API contract can evolve independently of internal
 * representation.
 */
public record OrderResponse(
        UUID orderId,
        String customerId,
        OrderStatus status,
        List<OrderLineItem> items,
        long totalAmountCents
) {

    /**
     * Builds an {@code OrderResponse} from a rehydrated {@link Order}
     * aggregate.
     */
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus(),
                order.getItems(),
                order.getTotalAmountCents()
        );
    }
}
