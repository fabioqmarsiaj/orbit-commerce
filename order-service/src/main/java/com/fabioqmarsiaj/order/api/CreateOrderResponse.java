package com.fabioqmarsiaj.order.api;

import java.util.UUID;

/**
 * Response body for {@code POST /orders}.
 */
public record CreateOrderResponse(UUID orderId) {
}
