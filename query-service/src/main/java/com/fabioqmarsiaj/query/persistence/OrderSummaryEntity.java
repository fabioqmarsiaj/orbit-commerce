package com.fabioqmarsiaj.query.persistence;

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
 * JPA entity backing the one-row-per-order read model powering
 * {@code GET /orders?status=}.
 *
 * <p>Projected exclusively from {@code order.events} (see
 * {@link OrderSummaryStatus}'s Javadoc for why this is a deliberate
 * simplification, not an oversight) — {@code inventory.events}/
 * {@code payment.events}/{@code shipping.events} are consumed by other
 * listeners purely to populate the granular {@code TimelineEntryEntity}
 * table, never to update this one. This avoids a real race that would
 * otherwise exist: since Kafka topics are independent of each other, an
 * event on e.g. {@code inventory.events} could theoretically be consumed
 * and processed before the corresponding {@code OrderCreated} on
 * {@code order.events} has been — projecting status from a SINGLE topic
 * (which is partitioned/ordered per {@code orderId}, same as every other
 * topic in this project) sidesteps that entirely.
 *
 * <p>Keyed by {@code orderId} directly (natural key — one summary row
 * per order). Like every other non-null-at-construction {@code @Id} in
 * this project, implements {@link Persistable} via the
 * {@code @Transient isNew} flag technique: this row IS updated later
 * (CREATED -> COMPLETED/CANCELLED/FAILED), so {@code isNew()} can't
 * simply always return {@code true} the way {@link TimelineEntryEntity}
 * (genuinely append-only) does.
 */
@Entity
@Table(name = "order_summaries")
public class OrderSummaryEntity implements Persistable<UUID> {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(name = "total_amount_cents", nullable = false)
    private long totalAmountCents;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OrderSummaryStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Transient
    private boolean isNew = false;

    protected OrderSummaryEntity() {
        // required by JPA
    }

    /** Used when projecting the initial {@code OrderCreated} event. */
    public OrderSummaryEntity(UUID orderId, String customerId, long totalAmountCents, Instant occurredAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.totalAmountCents = totalAmountCents;
        this.status = OrderSummaryStatus.CREATED;
        this.createdAt = occurredAt;
        this.updatedAt = occurredAt;
        this.isNew = true;
    }

    public void markCompleted(Instant occurredAt) {
        this.status = OrderSummaryStatus.COMPLETED;
        this.updatedAt = occurredAt;
    }

    public void markCancelled(Instant occurredAt) {
        this.status = OrderSummaryStatus.CANCELLED;
        this.updatedAt = occurredAt;
    }

    public void markFailed(Instant occurredAt) {
        this.status = OrderSummaryStatus.FAILED;
        this.updatedAt = occurredAt;
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

    public String getCustomerId() {
        return customerId;
    }

    public long getTotalAmountCents() {
        return totalAmountCents;
    }

    public OrderSummaryStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
