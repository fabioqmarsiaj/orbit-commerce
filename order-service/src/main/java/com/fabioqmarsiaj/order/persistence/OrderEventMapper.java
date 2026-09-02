package com.fabioqmarsiaj.order.persistence;

import com.fabioqmarsiaj.order.domain.event.OrderCancelled;
import com.fabioqmarsiaj.order.domain.event.OrderCompleted;
import com.fabioqmarsiaj.order.domain.event.OrderCreated;
import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.domain.event.OrderFailed;
import com.fabioqmarsiaj.order.domain.event.PaymentApproved;
import com.fabioqmarsiaj.order.domain.event.PaymentDeclined;
import com.fabioqmarsiaj.order.domain.event.ShipmentCreated;
import com.fabioqmarsiaj.order.domain.event.ShipmentFailed;
import com.fabioqmarsiaj.order.domain.event.StockRejected;
import com.fabioqmarsiaj.order.domain.event.StockReserved;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Converts between {@link OrderDomainEvent} records (used in-memory by the
 * {@code Order} aggregate) and their JSON representation stored in the
 * {@code payload} column of {@link OrderEventEntity}.
 *
 * <p>The {@code eventType} string (the simple class name, e.g.
 * {@code "OrderCreated"}) is stored alongside the JSON payload precisely so
 * that {@link #toDomainEvent} knows which record type to deserialize into —
 * plain JSON alone doesn't carry that information back.
 *
 * <p>This is deliberately a hand-written switch rather than relying on
 * Jackson's polymorphic type handling (e.g. {@code @JsonTypeInfo}) so that
 * the mapping between event type name and Java class stays explicit and
 * easy to trace — worth comparing both approaches once this works, as a
 * learning exercise.
 */
@Component
public class OrderEventMapper {

    private final ObjectMapper objectMapper;

    public OrderEventMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * @return the simple class name of the event, to store as the
     *         {@code eventType} discriminator column.
     */
    public String toEventType(OrderDomainEvent event) {
        // TODO: return event.getClass().getSimpleName()
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Serializes a domain event record to its JSON payload representation.
     */
    public String toPayload(OrderDomainEvent event) {
        // TODO: use objectMapper.writeValueAsString(event), wrapping the
        //  checked JsonProcessingException in a RuntimeException (this
        //  should never realistically fail for our own records, so a
        //  runtime exception is an acceptable simplification here).
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Deserializes a stored (eventType, payload) pair back into the
     * correct {@link OrderDomainEvent} subtype.
     */
    public OrderDomainEvent toDomainEvent(String eventType, String payload) {
        // TODO: implement a switch on eventType (a plain String switch,
        //  since we don't have the sealed type available at this point),
        //  calling objectMapper.readValue(payload, <Type>.class) for the
        //  matching record class, e.g.:
        //
        // return switch (eventType) {
        //     case "OrderCreated" -> readValue(payload, OrderCreated.class);
        //     case "StockReserved" -> readValue(payload, StockReserved.class);
        //     ... one case per permitted subtype of OrderDomainEvent ...
        //     default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
        // };
        throw new UnsupportedOperationException("not implemented yet");
    }
}
