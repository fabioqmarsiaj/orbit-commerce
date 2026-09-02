package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.shipping.ShipmentCreated;
import com.fabioqmarsiaj.events.shipping.ShipmentFailed;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Listens to {@code shipping.events}, reacting to {@link ShipmentCreated}
 * (happy path, order completes) and {@link ShipmentFailed} (final
 * compensation path, order fails).
 *
 * <p>See {@link InventoryEventListener} for why this uses the class-level
 * {@code @KafkaListener} + method-level {@code @KafkaHandler} pattern.
 */
@Component
@KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS, groupId = "order-service")
public class ShippingEventListener {

    private final OrderCommandService commandService;

    public ShippingEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onShipmentCreated(ShipmentCreated event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handleShipmentCreated(orderId, event.getShipmentId());
    }

    @KafkaHandler
    public void onShipmentFailed(ShipmentFailed event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handleShipmentFailed(orderId, event.getReason());
    }
}
