package com.fabioqmarsiaj.order.domain;

/**
 * The lifecycle states of an {@link Order} aggregate, as it moves through the
 * order fulfillment Saga.
 *
 * <pre>
 *   CREATED --(stock reserved)--> STOCK_RESERVED --(payment approved)--> PAYMENT_APPROVED
 *      |                              |                                      |
 *      | (stock rejected)             | (payment declined)                  | (shipment created)
 *      v                              v                                      v
 *   CANCELLED                     CANCELLED                              SHIPPED --> COMPLETED
 *                                                                            |
 *                                                                            | (shipment failed)
 *                                                                            v
 *                                                                          FAILED
 * </pre>
 */
public enum OrderStatus {
    CREATED,
    STOCK_RESERVED,
    PAYMENT_APPROVED,
    SHIPPED,
    COMPLETED,
    CANCELLED,
    FAILED
}
