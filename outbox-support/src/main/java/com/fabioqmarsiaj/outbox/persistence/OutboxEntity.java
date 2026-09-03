package com.fabioqmarsiaj.outbox.persistence;

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
 * JPA entity backing the transactional outbox table, shared by every
 * service in this project that needs to publish integration events
 * reliably (order-service, inventory-service, and eventually
 * payment-service/shipping-service).
 *
 * <p>Originally implemented independently inside order-service and then
 * copy-pasted into inventory-service for Phase 3, then extracted here once
 * a third near-identical copy started to look inevitable — see
 * {@code docs/decisions.md} for the extraction writeup. The class itself
 * is unchanged from its original form.
 *
 * <p>The Outbox pattern solves the "dual write" problem: if we tried to
 * both (a) save domain state to Postgres and (b) publish an event to Kafka
 * as two separate operations, a crash between them would leave the system
 * inconsistent (e.g. an order was created in the DB, but no one downstream
 * ever finds out, because the Kafka publish never happened).
 *
 * <p>Instead, we write the "intent to publish" as a row in this table, in
 * the SAME database transaction as the domain state change. A separate,
 * out-of-band poller ({@code AbstractOutboxPublisher}) periodically reads
 * {@code PENDING} rows and actually publishes them to Kafka, then marks
 * them {@code PUBLISHED}. This guarantees at-least-once delivery: either
 * the whole transaction (domain write + outbox write) commits together, or
 * neither does — there's no way to persist the domain change without also
 * recording the intent to publish.
 *
 * <p>This entity IS updated later — the publisher calls
 * {@link #markPublished} and saves the row again. So {@link #isNew()}
 * can't simply always return {@code true} (contrast with a truly
 * append-only entity like order-service's {@code OrderEventEntity});
 * instead we track it with a {@code @Transient} flag: set to
 * {@code true} only by the "brand new row" constructor. Rows loaded back
 * from the database go through JPA's no-arg constructor + field
 * reflection (bypassing our constructor entirely), so the flag naturally
 * stays at its default ({@code false}) for those, correctly telling
 * Spring Data to {@code merge()} (UPDATE) rather than {@code persist()}
 * (INSERT) when we save the row a second time.
 *
 * <p>Every table backed by this entity is named {@code outbox}, one per
 * service's own database (order_db, inventory_db, ...) — see
 * {@code docs/decisions.md} for why a shared outbox table/module doesn't
 * (and can't) mean a shared database: the whole point of the pattern is
 * atomicity with the SAME transaction as the domain write, which only
 * works if the outbox table lives in that same service's own database.
 *
 * @see OutboxStatus
 */
@Entity
@Table(name = "outbox")
public class OutboxEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    /** The aggregate (e.g. orderId) this outbox message relates to (used as the Kafka partition key). */
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
     * The event data as a JSON string (Avro's own schema-aware JSON codec,
     * not plain Jackson — see {@code OutboxRecorder} for why). Deliberately
     * NOT annotated with {@code @Lob}: on PostgreSQL, Hibernate maps a
     * {@code @Lob String} to the "oid" large-object type, which requires
     * an active transaction to even read (breaks any read outside a
     * transaction, e.g. auto-commit reads). Plain {@code TEXT} does not
     * have that restriction and is the correct mapping for JSON-sized text
     * payloads.
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

    /** See class javadoc for why this exists and how it's kept correct. */
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
     * Marks this row as published. Called by the outbox publisher after a
     * successful send to Kafka.
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
