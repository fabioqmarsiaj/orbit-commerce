package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.persistence.OrderEventEntity;
import com.fabioqmarsiaj.order.persistence.OrderEventMapper;
import com.fabioqmarsiaj.order.persistence.OrderEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Bridges the {@code Order} aggregate (in-memory, event-sourced) with the
 * {@code order_events} table (the durable event store).
 *
 * <p>This is intentionally kept separate from {@link OrderEventRepository}
 * (which only knows about {@link OrderEventEntity} rows) and from
 * {@link OrderEventMapper} (which only knows how to (de)serialize a single
 * event). {@code OrderEventStore} composes both to offer two operations
 * that speak the language of the domain: "load this order's full history"
 * and "append these new events to this order's history".
 */
@Component
public class OrderEventStore {

    private final OrderEventRepository repository;
    private final OrderEventMapper mapper;

    public OrderEventStore(OrderEventRepository repository, OrderEventMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * Loads and rehydrates the {@code Order} aggregate identified by
     * {@code orderId} by reading its full event history and replaying it
     * through {@link Order#rehydrate}.
     *
     * @return the rebuilt order, or {@code null} if no events exist for
     *         this id (consider returning an {@code Optional<Order>}
     *         instead once you get to wiring this into
     *         {@code OrderCommandService} — think about which is more
     *         idiomatic here).
     */
    public Order load(UUID orderId) {
        // TODO:
        //  1. repository.findByOrderIdOrderBySequenceNumberAsc(orderId)
        //  2. Map each OrderEventEntity -> OrderDomainEvent via
        //     mapper.toDomainEvent(entity.getEventType(), entity.getPayload())
        //  3. Order.rehydrate(...) the resulting list.
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Persists a batch of newly-raised domain events for one order,
     * assigning them consecutive sequence numbers that continue on from
     * whatever is already stored (0 if this is a brand-new order).
     *
     * <p>Callers are expected to invoke this within an existing
     * {@code @Transactional} boundary (see {@code OrderCommandService}),
     * in the SAME transaction where outbox rows are written — this is
     * what gives us the atomicity the Outbox pattern relies on.
     */
    public void append(UUID orderId, List<OrderDomainEvent> newEvents) {
        // TODO:
        //  1. Find how many events already exist for this order (hint:
        //     repository.findByOrderIdOrderBySequenceNumberAsc(orderId).size(),
        //     or add a dedicated count query to the repository if you'd
        //     rather avoid loading full entities just to count them).
        //  2. For each event in newEvents, in order, build an
        //     OrderEventEntity (new UUID id, orderId, next sequence
        //     number, mapper.toEventType(event), mapper.toPayload(event),
        //     event.occurredAt()) and repository.save(...) it.
        throw new UnsupportedOperationException("not implemented yet");
    }
}
