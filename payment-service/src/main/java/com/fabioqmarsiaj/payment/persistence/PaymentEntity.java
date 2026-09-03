package com.fabioqmarsiaj.payment.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity tracking the outcome of processing a payment for one order.
 *
 * <p>Unlike inventory-service's {@code stock_reservations} table (which is
 * pure audit — the compensating {@code ReleaseStockCommand} already
 * carries everything needed to release stock, so nothing actually depends
 * on reading that table back), this table is load-bearing: the
 * {@code RefundPaymentCommand} Avro schema carries only {@code orderId}
 * and {@code amountCents} — NOT the {@code paymentId} that
 * payment-service itself generated when the payment was approved (see
 * {@code PaymentApproved.paymentId} in event-schemas). Without persisting
 * that {@code paymentId} (and the fact that this order's payment was ever
 * approved in the first place) somewhere, a later refund command would
 * have no way to know whether refunding is even valid. See
 * {@code docs/decisions.md} for the fuller comparison with
 * inventory-service's audit-only reservation table.
 *
 * <p>Keyed by {@code orderId} directly (a natural key — one payment
 * attempt per order in this simplified simulation, not a randomly
 * generated id) rather than its own UUID. Like every other entity in this
 * project with a non-null-at-construction {@code @Id} (see
 * {@code order-service}'s {@code OutboxEntity} and inventory-service's
 * {@code StockEntity}/{@code StockReservationEntity}), it implements
 * {@link Persistable} using the {@code @Transient isNew} flag technique:
 * this row IS updated later (APPROVED/DECLINED -> REFUNDED), so
 * {@link #isNew()} can't simply always return {@code true}.
 */
@Entity
@Table(name = "payments")
public class PaymentEntity implements Persistable<UUID> {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    /**
     * Only meaningful when {@link #status} is {@link PaymentStatus#APPROVED}
     * or {@link PaymentStatus#REFUNDED} — {@code null} for
     * {@link PaymentStatus#DECLINED} rows, since no payment was actually
     * taken.
     */
    @Column(name = "payment_id", length = 100)
    private String paymentId;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "refunded_at")
    private Instant refundedAt;

    @Transient
    private boolean isNew = false;

    protected PaymentEntity() {
        // required by JPA
    }

    /**
     * Used when recording the outcome of a {@code ProcessPaymentCommand}
     * — either an approval ({@code paymentId} non-null) or a decline
     * ({@code paymentId} null).
     */
    public PaymentEntity(UUID orderId, String paymentId, long amountCents, PaymentStatus status) {
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amountCents = amountCents;
        this.status = status;
        this.createdAt = Instant.now();
        this.isNew = true;
    }

    /** Called when a {@code RefundPaymentCommand} is successfully processed. */
    public void markRefunded() {
        this.status = PaymentStatus.REFUNDED;
        this.refundedAt = Instant.now();
    }

    @Override
    public UUID getId() {
        return orderId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getRefundedAt() {
        return refundedAt;
    }
}
