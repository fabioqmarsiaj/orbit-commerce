package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Listens to {@code payment.events}, reacting to {@link PaymentApproved}
 * and {@link PaymentDeclined}. (A third possible message,
 * {@code PaymentRefunded}, is a compensation ack that order-service does
 * not need to react to.)
 *
 * <p>See {@link InventoryEventListener} for why this uses the class-level
 * {@code @KafkaListener} + method-level {@code @KafkaHandler} pattern.
 */
@Component
@KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "order-service")
public class PaymentEventListener {

    private final OrderCommandService commandService;

    public PaymentEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onPaymentApproved(PaymentApproved event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handlePaymentApproved(orderId, event.getPaymentId());
    }

    @KafkaHandler
    public void onPaymentDeclined(PaymentDeclined event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handlePaymentDeclined(orderId, event.getReason());
    }
}
