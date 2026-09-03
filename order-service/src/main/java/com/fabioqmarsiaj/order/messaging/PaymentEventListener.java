package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.events.payment.PaymentRefunded;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Listens to {@code payment.events}, reacting to all three message types
 * published there: {@link PaymentApproved}/{@link PaymentDeclined}
 * (Saga-driving) and {@link PaymentRefunded} (a compensation ack — the
 * order has already moved to {@code FAILED} by the time it arrives, so
 * there's nothing further to drive forward, only worth logging).
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — see {@link InventoryEventListener}'s
 * Javadoc for the full explanation of why (an unhandled type doesn't get
 * silently skipped, it throws and wedges the partition for this consumer
 * group), discovered live via the equivalent bug for
 * {@code StockReleased}. {@code PaymentRefunded} is added proactively here
 * rather than waiting to hit the same bug once shipping-service (Phase 5)
 * makes {@code handleShipmentFailed} — and therefore a real
 * {@code RefundPaymentCommand}/{@code PaymentRefunded} — reachable
 * end-to-end.
 */
@Slf4j
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

    /**
     * Compensation acknowledgment — the Saga doesn't advance any further
     * in reaction to this; the order is already in a terminal state
     * ({@code FAILED}) by the time the payment has been refunded.
     * Consumed purely so the partition offset advances instead of wedging
     * (see class Javadoc).
     */
    @KafkaHandler
    public void onPaymentRefunded(PaymentRefunded event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        log.info("Order {}: payment refunded (compensation ack) - Saga ends here for this branch", orderId);
    }
}
