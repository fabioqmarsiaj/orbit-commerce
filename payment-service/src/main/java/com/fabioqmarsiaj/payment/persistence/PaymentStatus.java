package com.fabioqmarsiaj.payment.persistence;

/**
 * Status of a {@link PaymentEntity} row.
 */
public enum PaymentStatus {
    /** Payment was approved (amount within the configured limit). */
    APPROVED,
    /** Payment was declined (amount exceeded the configured limit). */
    DECLINED,
    /** A previously approved payment was later refunded (compensation). */
    REFUNDED
}
