package com.fabioqmarsiaj.outbox.persistence;

/**
 * Publishing status of an {@link OutboxEntity} row.
 */
public enum OutboxStatus {
    /** Written to the outbox table but not yet published to Kafka. */
    PENDING,
    /** Successfully published to Kafka by the outbox publisher poller. */
    PUBLISHED
}
