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
        // TODO: build a new ReserveStockCommand(eventId, orderId, items, occurredAt).
        //  - eventId: UUID.randomUUID().toString()
        //  - orderId: order.getId().toString()
        //  - items: map order.getItems() (List<OrderLineItem>) to
        //    List<com.fabioqmarsiaj.events.order.OrderLineItem>
        //  - occurredAt: Instant.now()
        throw new UnsupportedOperationException("not implemented yet");
    }

    public ReleaseStockCommand releaseStock(Order order) {
        // TODO: same shape as reserveStock, but building a ReleaseStockCommand
        //  (used for compensation when payment is declined or shipment fails).
        throw new UnsupportedOperationException("not implemented yet");
    }

    public ProcessPaymentCommand processPayment(Order order) {
        // TODO: build a new ProcessPaymentCommand(eventId, orderId, customerId,
        //  amountCents, occurredAt) using order.getCustomerId() and
        //  order.getTotalAmountCents().
        throw new UnsupportedOperationException("not implemented yet");
    }

    public RefundPaymentCommand refundPayment(Order order) {
        // TODO: build a new RefundPaymentCommand(eventId, orderId, amountCents,
        //  occurredAt) — used for compensation when the shipment fails after
        //  payment was already approved.
        throw new UnsupportedOperationException("not implemented yet");
    }

    public CreateShipmentCommand createShipment(Order order) {
        // TODO: build a new CreateShipmentCommand(eventId, orderId, customerId,
        //  occurredAt).
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Shared helper to convert domain-model line items to their Avro
     * equivalent — same conversion needed in {@link OrderEventTranslator},
     * consider whether this duplication is worth extracting to a common
     * place once both are implemented.
     */
    private List<com.fabioqmarsiaj.events.order.OrderLineItem> toAvroLineItems(List<OrderLineItem> items) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
