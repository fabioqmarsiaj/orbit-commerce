package com.fabioqmarsiaj.order.domain;

/**
 * Value object representing a single line item within an {@link Order}.
 *
 * @param productId      the product being ordered
 * @param quantity       how many units were ordered
 * @param unitPriceCents the price of a single unit, in cents (to avoid floating point issues)
 */
public record OrderLineItem(String productId, int quantity, long unitPriceCents) {

    // TODO: add compact constructor validation, e.g.:
    //  - quantity must be > 0
    //  - unitPriceCents must be >= 0
    //  - productId must not be blank

    /**
     * @return the subtotal for this line item (quantity * unitPriceCents).
     */
    public long subtotalCents() {
        // TODO: implement
        throw new UnsupportedOperationException("not implemented yet");
    }
}
