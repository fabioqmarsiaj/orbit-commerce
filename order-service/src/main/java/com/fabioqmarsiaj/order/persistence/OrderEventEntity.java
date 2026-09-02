package com.fabioqmarsiaj.order.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity backing the append-only event store for {@code Order}
 * aggregates.
 *
 * <p>Rows in this table are NEVER updated or deleted — the whole point of
 * Event Sourcing is that this table is the immutable source of truth. The
 * current state of an order is always derived by reading all rows for a
 * given {@code orderId}, ordered by {@code sequenceNumber}, and replaying
 * them through {@link com.fabioqmarsiaj.order.domain.Order#rehydrate}.
 *
 * <p>The domain event itself (e.g. {@code OrderCreated}) is stored as a
 * JSON blob in {@code payload}, with {@code eventType} acting as a
 * discriminator so we know which record type to deserialize it back into.
 * See {@link OrderEventMapper}.
 *
 * <p>The unique constraint on ({@code order_id}, {@code sequence_number})
 * is a cheap optimistic-concurrency safety net: if two concurrent writers
 * both believe they're appending sequence number 5 for the same order, the
 * database will reject the second insert instead of silently corrupting
 * the event stream.
 */
@Entity
@Table(
        name = "order_events",
        uniqueConstraints = @UniqueConstraint(columnNames = {"order_id", "sequence_number"})
)
public class OrderEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "sequence_number", nullable = false)
    private long sequenceNumber;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected OrderEventEntity() {
        // required by JPA
    }

    public OrderEventEntity(UUID id, UUID orderId, long sequenceNumber, String eventType,
                             String payload, Instant occurredAt) {
        this.id = id;
        this.orderId = orderId;
        this.sequenceNumber = sequenceNumber;
        this.eventType = eventType;
        this.payload = payload;
        this.occurredAt = occurredAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}
