package com.fabioqmarsiaj.query.persistence;

/**
 * Status of an {@link OrderSummaryEntity} row.
 *
 * <p>Deliberately only has FOUR values, not order-service's full
 * seven-state {@code OrderStatus} ({@code CREATED}, {@code
 * STOCK_RESERVED}, {@code PAYMENT_APPROVED}, {@code SHIPPED}, {@code
 * COMPLETED}, {@code CANCELLED}, {@code FAILED}). {@code order.events}
 * (the only topic {@link OrderSummaryEntity} is projected from — see its
 * Javadoc) only ever publishes {@code OrderCreated}/{@code
 * OrderCompleted}/{@code OrderCancelled}/{@code OrderFailed}; the three
 * intermediate transitions ({@code STOCK_RESERVED}, {@code
 * PAYMENT_APPROVED}, {@code SHIPPED}) are never published as integration
 * events by order-service — they only ever exist inside order-service's
 * own {@code order_events} event store. So this read model genuinely
 * cannot represent those states; see {@code docs/decisions.md} ("Phase 7
 * — query-service") for the fuller design tradeoff (this is a
 * deliberate simplicity choice, not an oversight — the full, granular
 * blow-by-blow IS still visible via {@code GET /orders/{id}/timeline},
 * which also consumes {@code inventory.events}/{@code payment.events}/
 * {@code shipping.events}).
 */
public enum OrderSummaryStatus {
    CREATED,
    COMPLETED,
    CANCELLED,
    FAILED
}
