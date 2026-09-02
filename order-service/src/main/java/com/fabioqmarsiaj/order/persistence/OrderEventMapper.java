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
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Converts between {@link OrderDomainEvent} records (used in-memory by the
 * {@code Order} aggregate) and their JSON representation stored in the
 * {@code payload} column of {@link OrderEventEntity}.
 *
 * <p>Note: Spring Boot 4 defaults to <b>Jackson 3</b>
 * ({@code tools.jackson.databind.ObjectMapper}), not the classic Jackson 2
 * ({@code com.fasterxml.jackson.databind.ObjectMapper}) most existing
 * tutorials still reference — Jackson 2 support ships only in a deprecated
 * form. The autowired {@link ObjectMapper} bean here is the Jackson 3 one,
 * auto-configured by {@code spring-boot-starter-jackson} (a transitive
 * dependency of {@code spring-boot-starter-webmvc}). One practical
 * consequence: Jackson 3's {@code JacksonException} is an unchecked
 * exception, so no {@code try/catch} is needed around
 * {@code writeValueAsString}/{@code readValue} purely for compilation —
 * though we still let it propagate as-is on unexpected failures.
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
        return event.getClass().getSimpleName();
    }

    /**
     * Serializes a domain event record to its JSON payload representation.
     */
    public String toPayload(OrderDomainEvent event) {
        return objectMapper.writeValueAsString(event);
    }

    /**
     * Deserializes a stored (eventType, payload) pair back into the
     * correct {@link OrderDomainEvent} subtype.
     */
    public OrderDomainEvent toDomainEvent(String eventType, String payload) {
        return switch (eventType) {
            case "OrderCreated" -> objectMapper.readValue(payload, OrderCreated.class);
            case "StockReserved" -> objectMapper.readValue(payload, StockReserved.class);
            case "StockRejected" -> objectMapper.readValue(payload, StockRejected.class);
            case "PaymentApproved" -> objectMapper.readValue(payload, PaymentApproved.class);
            case "PaymentDeclined" -> objectMapper.readValue(payload, PaymentDeclined.class);
            case "ShipmentCreated" -> objectMapper.readValue(payload, ShipmentCreated.class);
            case "ShipmentFailed" -> objectMapper.readValue(payload, ShipmentFailed.class);
            case "OrderCancelled" -> objectMapper.readValue(payload, OrderCancelled.class);
            case "OrderCompleted" -> objectMapper.readValue(payload, OrderCompleted.class);
            case "OrderFailed" -> objectMapper.readValue(payload, OrderFailed.class);
            default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
        };
    }
}
