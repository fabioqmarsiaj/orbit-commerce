package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.inventory.ReleaseStockCommand;
import com.fabioqmarsiaj.events.inventory.ReserveStockCommand;
import com.fabioqmarsiaj.events.payment.ProcessPaymentCommand;
import com.fabioqmarsiaj.events.payment.RefundPaymentCommand;
import com.fabioqmarsiaj.events.shipping.CreateShipmentCommand;
import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Builds the Avro command messages that order-service, as the Saga
 * orchestrator, sends to the participant services (inventory-service,
 * payment-service, shipping-service) to move the Saga forward or to
 * compensate a previous step.
 *
 * <p>Unlike domain events (which are appended to the event store) and
 * integration events (which flow through the outbox), these commands are
 * produced directly by {@code OrderCommandService} as an explicit,
 * synchronous-looking side effect of reacting to an incoming event —
 * worth discussing during review whether these should ALSO go through the
 * outbox for full consistency, or whether "at least once, fire and forget"
 * is an acceptable simplification for commands specifically. Come back to
 * this question once the rest of the Saga works end to end.
 */
@Component
public class SagaCommandFactory {

    public ReserveStockCommand reserveStock(Order order) {
        return new ReserveStockCommand(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                toAvroLineItems(order.getItems()),
                Instant.now());
    }

    public ReleaseStockCommand releaseStock(Order order) {
        return new ReleaseStockCommand(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                toAvroLineItems(order.getItems()),
                Instant.now());
    }

    public ProcessPaymentCommand processPayment(Order order) {
        return new ProcessPaymentCommand(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                order.getCustomerId(),
                order.getTotalAmountCents(),
                Instant.now());
    }

    public RefundPaymentCommand refundPayment(Order order) {
        return new RefundPaymentCommand(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                order.getTotalAmountCents(),
                Instant.now());
    }

    public CreateShipmentCommand createShipment(Order order) {
        return new CreateShipmentCommand(
                UUID.randomUUID().toString(),
                order.getId().toString(),
                order.getCustomerId(),
                Instant.now());
    }

    /**
     * Shared helper to convert domain-model line items to their Avro
     * equivalent — same conversion needed in {@link OrderEventTranslator},
     * kept duplicated for now since both classes are small and independent;
     * worth extracting to a common place if a third use case appears.
     */
    private List<com.fabioqmarsiaj.events.order.OrderLineItem> toAvroLineItems(List<OrderLineItem> items) {
        return items.stream()
                .map(i -> new com.fabioqmarsiaj.events.order.OrderLineItem(
                        i.productId(), i.quantity(), i.unitPriceCents()))
                .toList();
    }
}
