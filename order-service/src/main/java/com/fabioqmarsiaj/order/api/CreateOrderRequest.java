package com.fabioqmarsiaj.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.List;

/**
 * Request body for {@code POST /orders}.
 */
public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty @Valid List<LineItemRequest> items
) {

    /**
     * @param productId      the product being ordered
     * @param quantity       units ordered (must be positive)
     * @param unitPriceCents price per unit, in cents (must be non-negative)
     */
    public record LineItemRequest(
            @NotBlank String productId,
            @Positive int quantity,
            @PositiveOrZero long unitPriceCents
    ) {
    }
}
