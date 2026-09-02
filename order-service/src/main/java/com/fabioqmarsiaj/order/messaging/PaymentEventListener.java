package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code payment.events}, reacting to {@link PaymentApproved}
 * and {@link PaymentDeclined}. (A third possible message,
 * {@code PaymentRefunded}, is a compensation ack that order-service does
 * not need to react to.)
 */
@Component
public class PaymentEventListener {

    private final OrderCommandService commandService;

    public PaymentEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "order-service")
    public void onPaymentApproved(PaymentApproved event) {
        // TODO: parse event.getOrderId(), then call
        //  commandService.handlePaymentApproved(orderId, event.getPaymentId()).
        throw new UnsupportedOperationException("not implemented yet");
    }

    public void onPaymentDeclined(PaymentDeclined event) {
        // TODO: parse event.getOrderId(), then call
        //  commandService.handlePaymentDeclined(orderId, event.getReason()).
        throw new UnsupportedOperationException("not implemented yet");
    }
}
