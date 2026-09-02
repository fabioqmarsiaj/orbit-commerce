package com.fabioqmarsiaj.inventory.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Audit/tracking record of one (order, product) stock reservation.
 *
 * <p>Not strictly required for correctness today: both
 * {@code ReserveStockCommand} and {@code ReleaseStockCommand} already carry
 * the full line-item list (productId + quantity) on the wire, so
 * inventory-service technically doesn't need to look anything up to know
 * how much to release. This table exists for auditability (what did we
 * actually reserve, and when) and as the natural place to hang future
 * idempotent-consumption logic (deduplicating by {@code eventId} or by
 * (orderId, productId) — deferred per {@code TASKS.md} Phase 3, P1).
 *
 * <p>App-assigned {@link UUID} id, same {@link Persistable} treatment as
 * {@code order-service}'s {@code OutboxEntity}: this row IS updated later
 * (RESERVED -> RELEASED), so {@link #isNew()} is backed by a
 * {@code @Transient} flag set only by the "new row" constructor, not
 * hardcoded to {@code true} like a truly append-only entity would be.
 */
@Entity
@Table(name = "stock_reservations")
public class StockReservationEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "product_id", nullable = false, length = 100)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private StockReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "released_at")
    private Instant releasedAt;

    @jakarta.persistence.Transient
    private boolean isNew = false;

    protected StockReservationEntity() {
        // required by JPA
    }

    public StockReservationEntity(UUID id, UUID orderId, String productId, int quantity) {
        this.id = id;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = StockReservationStatus.RESERVED;
        this.createdAt = Instant.now();
        this.isNew = true;
    }

    public void markReleased() {
        this.status = StockReservationStatus.RELEASED;
        this.releasedAt = Instant.now();
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public StockReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getReleasedAt() {
        return releasedAt;
    }
}
