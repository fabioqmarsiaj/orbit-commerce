package com.fabioqmarsiaj.order.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Marker sealed interface for every domain event that can happen to an
 * {@link com.fabioqmarsiaj.order.domain.Order} aggregate.
 *
 * <p>These events are the source of truth for the aggregate's state (Event
 * Sourcing): the current state of an {@code Order} is always derived by
 * replaying its events in order, never stored "as is" anywhere else.
 *
 * <p>Every permitted subtype must be a {@code record} so that {@code Order}
 * can use exhaustive pattern matching ({@code switch}) to apply each event
 * without needing a default branch — the compiler will force us to handle
 * every case if a new event type is ever added.
 *
 * <p>Note the distinction from the Avro classes in the {@code event-schemas}
 * module: those are the wire format published to Kafka (integration
 * events), while these records are the internal representation used to
 * rebuild aggregate state. A dedicated mapper translates between the two
 * worlds (see the {@code messaging} package).
 */
public sealed interface OrderDomainEvent
        permits
        OrderCreated,
        StockReserved,
        StockRejected,
        PaymentApproved,
        PaymentDeclined,
        ShipmentCreated,
        ShipmentFailed,
        OrderCancelled,
        OrderCompleted,
        OrderFailed {

    /**
     * @return a unique identifier for this specific event instance (useful for
     *         idempotency / deduplication).
     */
    UUID eventId();

    /**
     * @return the id of the {@code Order} aggregate this event belongs to.
     */
    UUID orderId();

    /**
     * @return the instant this event was recorded.
     */
    Instant occurredAt();
}
