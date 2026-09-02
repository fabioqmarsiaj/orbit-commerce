package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.shipping.ShipmentCreated;
import com.fabioqmarsiaj.events.shipping.ShipmentFailed;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code shipping.events}, reacting to {@link ShipmentCreated}
 * (happy path, order completes) and {@link ShipmentFailed} (final
 * compensation path, order fails).
 */
@Component
public class ShippingEventListener {

    private final OrderCommandService commandService;

    public ShippingEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS, groupId = "order-service")
    public void onShipmentCreated(ShipmentCreated event) {
        // TODO: parse event.getOrderId(), then call
        //  commandService.handleShipmentCreated(orderId, event.getShipmentId()).
        throw new UnsupportedOperationException("not implemented yet");
    }

    public void onShipmentFailed(ShipmentFailed event) {
        // TODO: parse event.getOrderId(), then call
        //  commandService.handleShipmentFailed(orderId, event.getReason()).
        throw new UnsupportedOperationException("not implemented yet");
    }
}
