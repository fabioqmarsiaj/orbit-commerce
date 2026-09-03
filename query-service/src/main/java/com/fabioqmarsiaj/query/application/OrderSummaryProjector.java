package com.fabioqmarsiaj.query.application;

import com.fabioqmarsiaj.query.persistence.OrderSummaryEntity;
import com.fabioqmarsiaj.query.persistence.OrderSummaryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Maintains the {@link OrderSummaryEntity} read model, projected
 * EXCLUSIVELY from {@code order.events} — see that entity's Javadoc for
 * why (avoiding a cross-topic race). Called only by
 * {@code OrderEventListener}, never by the Inventory/Payment/Shipping
 * listeners (which only feed {@code TimelineRecorder}).
 */
@Slf4j
@Component
public class OrderSummaryProjector {

    private final OrderSummaryRepository repository;

    public OrderSummaryProjector(OrderSummaryRepository repository) {
        this.repository = repository;
    }

    public void onOrderCreated(UUID orderId, String customerId, long totalAmountCents, Instant occurredAt) {
        repository.save(new OrderSummaryEntity(orderId, customerId, totalAmountCents, occurredAt));
    }

    public void onOrderCompleted(UUID orderId, Instant occurredAt) {
        withSummary(orderId, summary -> summary.markCompleted(occurredAt));
    }

    public void onOrderCancelled(UUID orderId, Instant occurredAt) {
        withSummary(orderId, summary -> summary.markCancelled(occurredAt));
    }

    public void onOrderFailed(UUID orderId, Instant occurredAt) {
        withSummary(orderId, summary -> summary.markFailed(occurredAt));
    }

    /**
     * Applies a status transition to an existing summary row, saving the
     * result. Logs (rather than throws) if the row doesn't exist yet —
     * defensively handled, since Kafka only guarantees at-least-once
     * delivery and ordering per-partition, not strict cross-consumer
     * causal ordering guarantees beyond that; a missing row here would
     * indicate a genuine anomaly worth knowing about, not a case to
     * silently ignore.
     */
    private void withSummary(UUID orderId, java.util.function.Consumer<OrderSummaryEntity> transition) {
        Optional<OrderSummaryEntity> summary = repository.findByOrderId(orderId);
        if (summary.isEmpty()) {
            log.warn("Order {}: received a terminal order.events transition but no OrderSummaryEntity exists yet - ignoring", orderId);
            return;
        }
        transition.accept(summary.get());
        repository.save(summary.get());
    }
}
