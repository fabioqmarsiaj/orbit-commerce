package com.fabioqmarsiaj.shipping.messaging;

import com.fabioqmarsiaj.events.shipping.CreateShipmentCommand;
import com.fabioqmarsiaj.shipping.application.ShippingCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code shipping.commands}, reacting to the single Saga
 * command type that flows through it: {@link CreateShipmentCommand}.
 * There's no compensating command for shipping-service to consume (a
 * shipment failure is terminal — order-service compensates by refunding
 * payment/releasing stock instead of asking shipping-service to undo
 * anything), unlike inventory-service (which also handles
 * {@code ReleaseStockCommand}) or payment-service (which also handles
 * {@code RefundPaymentCommand}).
 *
 * <p>Same class-level {@code @KafkaListener} + method-level
 * {@code @KafkaHandler} pattern used throughout this project — see
 * {@code order-service}'s {@code InventoryEventListener} Javadoc for why
 * this matters even with only one handler: if
 * {@code shipping.commands} ever grows a second command type, a matching
 * {@code @KafkaHandler} must be added here too, or the listener will
 * throw and wedge the partition on the first unhandled message (see
 * {@code docs/decisions.md}, "Post-Phase 4 fix").
 */
@Component
@KafkaListener(topics = KafkaTopics.SHIPPING_COMMANDS, groupId = "shipping-service")
public class ShippingCommandListener {

    private final ShippingCommandService commandService;

    public ShippingCommandListener(ShippingCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onCreateShipment(CreateShipmentCommand command) {
        commandService.handleCreateShipment(command);
    }
}
