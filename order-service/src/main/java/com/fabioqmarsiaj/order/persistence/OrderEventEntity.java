package com.fabioqmarsiaj.order.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.springframework.data.domain.Persistable;

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
 *
 * <p>This entity implements {@link Persistable} and always reports
 * {@link #isNew()} as {@code true}. Without it, Spring Data JPA infers
 * "new vs existing" purely from whether the {@code @Id} field is
 * {@code null} — but here {@code id} is a UUID assigned by application
 * code (via {@link java.util.UUID#randomUUID()}) BEFORE the entity is
 * constructed, so it's never null. Without {@code Persistable}, Spring Data
 * would wrongly treat every {@code save()} as updating an existing
 * (detached) row via {@code merge()}, which fails with an
 * {@code ObjectOptimisticLockingFailureException} since no such row
 * actually exists yet. Since this table is append-only, "always new" is
 * also simply the correct semantics.
 */
@Entity
@Table(
        name = "order_events",
        uniqueConstraints = @UniqueConstraint(columnNames = {"order_id", "sequence_number"})
)
public class OrderEventEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "sequence_number", nullable = false)
    private long sequenceNumber;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    // Deliberately NOT annotated with @Lob: on PostgreSQL, Hibernate maps a
    // @Lob String to the "oid" large-object type, which requires the whole
    // read to happen inside an active transaction (auto-commit reads throw
    // "Large Objects may not be used in auto-commit mode"). Since payloads
    // here are just JSON text of modest size, mapping directly to Postgres'
    // native TEXT column type avoids that restriction entirely.
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
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

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return true;
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
