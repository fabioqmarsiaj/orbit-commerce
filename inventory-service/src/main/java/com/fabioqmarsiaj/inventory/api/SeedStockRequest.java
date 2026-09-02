package com.fabioqmarsiaj.inventory.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Request body for {@code POST /stock}.
 *
 * <p>Upserts a product's available quantity — creates the product if it
 * doesn't exist yet, or resets its {@code quantity_available} if it does.
 * Deliberately not "add N units": this is a test-data seeding endpoint,
 * not a "receive shipment" domain operation.
 */
public record SeedStockRequest(
        @NotBlank String productId,
        @PositiveOrZero int quantity
) {
}
