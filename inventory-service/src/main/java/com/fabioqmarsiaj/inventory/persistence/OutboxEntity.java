package com.fabioqmarsiaj.inventory.persistence;

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
 * JPA entity backing the transactional outbox table for inventory-service.
 *
 * <p>Same Outbox pattern as {@code order-service}'s {@code OutboxEntity}
 * (see that class's Javadoc, and {@code docs/decisions.md}, for the full
 * "why" — the short version: writing the "intent to publish" as a row in
 * the same DB transaction as the domain state change guarantees the two
 * either both happen or neither does, avoiding the "dual write" problem).
 *
 * <p>There is deliberately no shared/reusable Outbox module across
 * services yet — each service (order-service, and now inventory-service)
 * implements its own copy of this entity/writer/publisher/repository. See
 * {@code docs/decisions.md} for the note on why that might be worth
 * revisiting later.
 */
@Entity
@Table(name = "outbox")
public class OutboxEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    /** The order this outbox message relates to (used as the Kafka partition key). */
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    /** The Kafka topic this message must be published to (e.g. "inventory.events"). */
    @Column(name = "topic", nullable = false, length = 200)
    private String topic;

    /**
     * Discriminator identifying which Avro schema/class the payload should
     * be deserialized into before publishing (e.g. "StockReserved").
     */
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    /**
     * The event data as a JSON string (Avro's own JSON codec, not plain
     * Jackson — see {@code OutboxWriter} for why). Deliberately NOT
     * annotated with {@code @Lob} — see {@code order-service}'s
     * equivalent note: on PostgreSQL, {@code @Lob String} maps to the
     * large-object type, which requires an active transaction to even
     * read.
     */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    /** See {@code order-service}'s OutboxEntity javadoc for why this exists. */
    @Transient
    private boolean isNew = false;

    protected OutboxEntity() {
        // required by JPA
    }

    public OutboxEntity(UUID id, UUID aggregateId, String topic, String eventType,
                         String payload, OutboxStatus status, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
        this.isNew = true;
    }

    /**
     * Marks this row as published. Called by {@code OutboxPublisher} after
     * a successful send to Kafka.
     */
    public void markPublished(Instant publishedAt) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedAt;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
