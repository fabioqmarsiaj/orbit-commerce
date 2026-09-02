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
     *         this id.
     */
    public Order load(UUID orderId) {
        List<OrderEventEntity> entities = repository.findByOrderIdOrderBySequenceNumberAsc(orderId);
        if (entities.isEmpty()) {
            return null;
        }

        List<OrderDomainEvent> events = entities.stream()
                .map(entity -> mapper.toDomainEvent(entity.getEventType(), entity.getPayload()))
                .toList();

        return Order.rehydrate(events);
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
        long nextSequenceNumber = repository.findByOrderIdOrderBySequenceNumberAsc(orderId).size();

        for (OrderDomainEvent event : newEvents) {
            OrderEventEntity entity = new OrderEventEntity(
                    UUID.randomUUID(),
                    orderId,
                    nextSequenceNumber,
                    mapper.toEventType(event),
                    mapper.toPayload(event),
                    event.occurredAt()
            );
            repository.save(entity);
            nextSequenceNumber++;
        }
    }
}
