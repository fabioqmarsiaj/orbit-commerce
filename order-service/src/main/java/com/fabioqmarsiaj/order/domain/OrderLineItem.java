package com.fabioqmarsiaj.order.domain;

/**
 * Value object representing a single line item within an {@link Order}.
 *
 * @param productId      the product being ordered
 * @param quantity       how many units were ordered
 * @param unitPriceCents the price of a single unit, in cents (to avoid floating point issues)
 */
public record OrderLineItem(String productId, int quantity, long unitPriceCents) {

    public OrderLineItem {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be greater than zero");
        }
        if (unitPriceCents < 0) {
            throw new IllegalArgumentException("unitPriceCents must not be negative");
        }
    }

    /**
     * @return the subtotal for this line item (quantity * unitPriceCents).
     */
    public long subtotalCents() {
        return (long) quantity * unitPriceCents;
    }
}
