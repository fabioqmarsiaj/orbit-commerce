package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.domain.event.OrderCancelled;
import com.fabioqmarsiaj.order.domain.event.OrderCompleted;
import com.fabioqmarsiaj.order.domain.event.OrderCreated;
import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.domain.event.OrderFailed;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Translates between the internal domain model and the public, wire-format
 * contract shared with other services (the Avro classes generated from
 * {@code event-schemas}).
 *
 * <p>This is the seam where "Event Sourcing" (internal, can change freely)
 * meets "integration events" (external contract, must stay backward
 * compatible). Only a subset of {@link OrderDomainEvent} subtypes actually
 * cross this seam:
 *
 * <ul>
 *   <li>{@link OrderCreated}, {@link OrderCompleted}, {@link OrderCancelled},
 *       {@link OrderFailed} are published on the {@code order.events} topic
 *       — other services care about the order's overall lifecycle.</li>
 *   <li>{@code StockReserved}, {@code StockRejected}, {@code PaymentApproved},
 *       {@code PaymentDeclined}, {@code ShipmentCreated}, {@code ShipmentFailed}
 *       are NOT re-published here — they were already published by their
 *       owning service (inventory-service, payment-service,
 *       shipping-service) on their own topics, and order-service only
 *       consumed them to react. Re-publishing them on order.events would be
 *       redundant.</li>
 * </ul>
 *
 * <p>Note the deliberate class name collision between
 * {@code com.fabioqmarsiaj.order.domain.event.OrderCreated} (this module,
 * internal) and {@code com.fabioqmarsiaj.events.order.OrderCreated} (the
 * {@code event-schemas} module, Avro-generated, public contract) — they
 * represent the same real-world fact but are intentionally two different
 * classes with two different reasons to change.
 */
@Component
public class OrderEventTranslator {

    /**
     * @return {@code true} if this domain event has a corresponding public
     *         integration event that should be published on
     *         {@code order.events}.
     */
    public boolean isPublishable(OrderDomainEvent event) {
        // TODO: return true only for OrderCreated / OrderCompleted /
        //  OrderCancelled / OrderFailed instances (e.g. via `instanceof`
        //  checks, or a pattern-matching switch with a default branch).
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Converts a publishable domain event into its Avro wire-format
     * equivalent. Callers must check {@link #isPublishable} first — this
     * method should throw for any other event type.
     */
    public SpecificRecord toAvro(OrderDomainEvent event) {
        // TODO: implement a pattern-matching switch, e.g.:
        //
        // return switch (event) {
        //     case OrderCreated e -> new com.fabioqmarsiaj.events.order.OrderCreated(
        //             e.eventId().toString(),
        //             e.orderId().toString(),
        //             e.customerId(),
        //             toAvroLineItems(e.items()),
        //             e.totalAmountCents(),
        //             e.occurredAt());
        //     case OrderCompleted e -> new com.fabioqmarsiaj.events.order.OrderCompleted(
        //             e.eventId().toString(), e.orderId().toString(), e.occurredAt());
        //     case OrderCancelled e -> new com.fabioqmarsiaj.events.order.OrderCancelled(
        //             e.eventId().toString(), e.orderId().toString(), e.reason(), e.occurredAt());
        //     case OrderFailed e -> new com.fabioqmarsiaj.events.order.OrderFailed(
        //             e.eventId().toString(), e.orderId().toString(), e.reason(), e.occurredAt());
        //     default -> throw new IllegalArgumentException(
        //             "Event type is not publishable to order.events: " + event.getClass());
        // };
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Converts domain-model line items to their Avro equivalent, used when
     * building an Avro {@code OrderCreated} record.
     */
    private List<com.fabioqmarsiaj.events.order.OrderLineItem> toAvroLineItems(List<OrderLineItem> items) {
        // TODO: items.stream()
        //  .map(i -> new com.fabioqmarsiaj.events.order.OrderLineItem(
        //          i.productId(), i.quantity(), i.unitPriceCents()))
        //  .toList();
        throw new UnsupportedOperationException("not implemented yet");
    }
}
