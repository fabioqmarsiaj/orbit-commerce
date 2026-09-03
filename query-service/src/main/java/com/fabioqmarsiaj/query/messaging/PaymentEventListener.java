package com.fabioqmarsiaj.query.messaging;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.events.payment.PaymentRefunded;
import com.fabioqmarsiaj.query.application.TimelineRecorder;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Listens to {@code payment.events}, feeding only
 * {@link TimelineRecorder} — see {@code InventoryEventListener}'s
 * Javadoc for why this topic never updates {@code OrderSummaryEntity}.
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — see order-service's
 * {@code InventoryEventListener} Javadoc for the full explanation of why.
 * All three types this topic carries are handled below. Note
 * {@code PaymentRefunded} carries no {@code paymentId}/amount field (see
 * event-schemas) — nothing to add to the timeline entry's fields beyond
 * the bare fact that it happened.
 */
@Component
@KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "query-service")
public class PaymentEventListener {

    private final TimelineRecorder timelineRecorder;

    public PaymentEventListener(TimelineRecorder timelineRecorder) {
        this.timelineRecorder = timelineRecorder;
    }

    @KafkaHandler
    public void onPaymentApproved(PaymentApproved event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, "PaymentApproved", Map.of(
                "paymentId", event.getPaymentId()
        ), event.getOccurredAt());
    }

    @KafkaHandler
    public void onPaymentDeclined(PaymentDeclined event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, "PaymentDeclined", Map.of(
                "reason", event.getReason()
        ), event.getOccurredAt());
    }

    @KafkaHandler
    public void onPaymentRefunded(PaymentRefunded event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, "PaymentRefunded", Map.of(), event.getOccurredAt());
    }
}
