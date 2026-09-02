package com.fabioqmarsiaj.order.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity backing the transactional outbox table.
 *
 * <p>The Outbox pattern solves the "dual write" problem: if we tried to
 * both (a) save domain state to Postgres and (b) publish an event to Kafka
 * as two separate operations, a crash between them would leave the system
 * inconsistent (e.g. the order was created in the DB, but no one downstream
 * ever finds out, because the Kafka publish never happened).
 *
 * <p>Instead, we write the "intent to publish" as a row in this table, in
 * the SAME database transaction as the domain state change (see
 * {@link OrderEventEntity} — both are written together). A separate,
 * out-of-band poller ({@code OutboxPublisher}) periodically reads
 * {@code PENDING} rows and actually publishes them to Kafka, then marks
 * them {@code PUBLISHED}. This guarantees at-least-once delivery: either
 * the whole transaction (domain write + outbox write) commits together, or
 * neither does — there's no way to persist the domain change without also
 * recording the intent to publish.
 *
 * @see OutboxStatus
 */
@Entity
@Table(name = "outbox")
public class OutboxEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /** The order this outbox message relates to (used as the Kafka partition key). */
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    /** The Kafka topic this message must be published to (e.g. "order.events"). */
    @Column(name = "topic", nullable = false, length = 200)
    private String topic;

    /**
     * Discriminator identifying which Avro schema/class the payload should
     * be deserialized into before publishing (e.g. "OrderCreated").
     */
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    /**
     * The event data as a JSON string. The {@code OutboxPublisher} is
     * responsible for turning this, together with {@code eventType}, back
     * into the correct Avro-generated class before sending it through a
     * Kafka producer configured with the Confluent Avro serializer.
     */
    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

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
    }

    /**
     * Marks this row as published. Called by {@code OutboxPublisher} after
     * a successful send to Kafka.
     */
    public void markPublished(Instant publishedAt) {
        // TODO: set status to OutboxStatus.PUBLISHED and record publishedAt.
        throw new UnsupportedOperationException("not implemented yet");
    }

    public UUID getId() {
        return id;
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
