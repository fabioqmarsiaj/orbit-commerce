package com.fabioqmarsiaj.query.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity backing the append-only cross-service order timeline read
 * model: one row per domain event consumed from ANY of the four Saga
 * event topics ({@code order.events}, {@code inventory.events},
 * {@code payment.events}, {@code shipping.events}).
 *
 * <p>Structurally similar to order-service's {@code OrderEventEntity}
 * (Phase 2) — same append-only, {@code Persistable<UUID>} always-new
 * shape — but conceptually different: {@code OrderEventEntity} is the
 * authoritative event-sourced source of truth for ONE aggregate within
 * ONE service; this table is a derived, denormalized, cross-service
 * READ model with no authority of its own — it exists purely to answer
 * {@code GET /orders/{id}/timeline} quickly, and could be dropped and
 * rebuilt by replaying the four topics from the beginning at any time
 * (the defining property of a CQRS read model).
 *
 * <p>{@code eventType} is the Avro record's simple name (e.g.
 * {@code "StockReserved"}) — matches the same discriminator convention
 * used by {@code order-service}'s {@code OrderEventEntity} and every
 * outbox entity in this project. {@code sourceTopic} additionally
 * records which of the four topics the event arrived on, since
 * {@code eventType} names alone don't indicate that (e.g. nothing about
 * the string {@code "OrderCreated"} says "this came from order.events" —
 * obvious for humans reading code, but not encoded in the stored row
 * otherwise).
 *
 * <p>{@code payload} stores a plain Jackson 3 JSON serialization of a
 * {@code Map<String,Object>} of the event's own fields (built by
 * {@code TimelineRecorder} from each listener) — NOT a serialization of
 * the raw Avro {@link org.apache.avro.specific.SpecificRecord} itself.
 * This sidesteps the same Avro-vs-Jackson pitfall documented since Phase
 * 2 (Avro-generated classes expose a bean-style {@code getSchema()}
 * getter that breaks generic Jackson introspection) without needing
 * Avro's own JSON codec here — unlike the outbox writers, this table is
 * never read back into a strongly-typed Avro object, only ever
 * deserialized into a generic {@code Map} for the timeline API response,
 * so the extra ceremony of Avro's schema-aware codec isn't warranted.
 */
@Entity
@Table(name = "timeline_entries")
public class TimelineEntryEntity implements Persistable<UUID> {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "source_topic", nullable = false, length = 100)
    private String sourceTopic;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    // Deliberately NOT annotated with @Lob — see order-service's
    // OrderEventEntity for the full explanation: on PostgreSQL, @Lob
    // String maps to the "oid" large-object type, which requires an
    // active transaction to even read.
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected TimelineEntryEntity() {
        // required by JPA
    }

    public TimelineEntryEntity(UUID id, UUID orderId, String sourceTopic, String eventType,
                                String payload, Instant occurredAt) {
        this.id = id;
        this.orderId = orderId;
        this.sourceTopic = sourceTopic;
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
        // Append-only table (see class Javadoc) — every save() is
        // genuinely a new row, same as order-service's OrderEventEntity.
        return true;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getSourceTopic() {
        return sourceTopic;
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
