package com.fabioqmarsiaj.order.persistence;

/**
 * Publishing status of an {@link OutboxEntity} row.
 */
public enum OutboxStatus {
    /** Written to the outbox table but not yet published to Kafka. */
    PENDING,
    /** Successfully published to Kafka by the {@code OutboxPublisher} poller. */
    PUBLISHED
}
