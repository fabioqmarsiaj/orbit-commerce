package com.fabioqmarsiaj.shipping.persistence;

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
 * JPA entity recording the outcome of a shipment creation attempt for one
 * order.
 *
 * <p>Unlike payment-service's {@code PaymentEntity} — which is
 * load-bearing, since a later {@code RefundPaymentCommand} needs to look
 * up the {@code paymentId} generated on approval — this table is pure
 * audit/query convenience, the same role inventory-service's
 * {@code stock_reservations} table plays. There is no compensating
 * command that flows back INTO shipping-service (no
 * {@code CancelShipmentCommand} exists in this Saga's design — a shipment
 * failure is terminal, handled entirely by order-service compensating
 * payment/inventory instead), so nothing in the Saga's correctness
 * depends on this row existing or being read back. It exists so
 * {@code GET /shipments/{orderId}} has something to return, and as the
 * natural place to hang future idempotent-consumption logic (deferred,
 * same as every other service's P1 "Idempotent consumption" item).
 *
 * <p>Keyed by {@code orderId} directly (natural key — one shipment
 * attempt per order in this simplified simulation), same
 * {@code Persistable<UUID>} + {@code @Transient isNew} treatment as every
 * other non-null-at-construction {@code @Id} in this project. Unlike
 * {@code PaymentEntity} (updated later: APPROVED/DECLINED -> REFUNDED),
 * this row is written once and never updated again — but it still can't
 * hardcode {@code isNew()} to {@code true} the way a genuinely
 * append-only, multi-row-per-key table (like order-service's
 * {@code OrderEventEntity}) can, since {@code orderId} is a single-row
 * natural key here, not a surrogate id on an ever-growing log.
 */
@Entity
@Table(name = "shipments")
public class ShipmentEntity implements Persistable<UUID> {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    /** Only meaningful when {@link #status} is {@link ShipmentStatus#CREATED}. */
    @Column(name = "shipment_id", length = 100)
    private String shipmentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ShipmentStatus status;

    /** Only meaningful when {@link #status} is {@link ShipmentStatus#FAILED}. */
    @Column(name = "reason", columnDefinition = "TEXT")
    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Transient
    private boolean isNew = false;

    protected ShipmentEntity() {
        // required by JPA
    }

    public ShipmentEntity(UUID orderId, String shipmentId, ShipmentStatus status, String reason) {
        this.orderId = orderId;
        this.shipmentId = shipmentId;
        this.status = status;
        this.reason = reason;
        this.createdAt = Instant.now();
        this.isNew = true;
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

    public String getShipmentId() {
        return shipmentId;
    }

    public ShipmentStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
