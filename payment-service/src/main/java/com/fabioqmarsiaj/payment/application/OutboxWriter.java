package com.fabioqmarsiaj.payment.application;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.events.payment.PaymentRefunded;
import com.fabioqmarsiaj.outbox.application.OutboxRecorder;
import com.fabioqmarsiaj.payment.messaging.KafkaTopics;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Writes outbox rows for payment-service's outbound integration events
 * ({@code PaymentApproved}/{@code PaymentDeclined}/{@code PaymentRefunded}).
 *
 * <p>The mechanical part (Avro-JSON encoding + building/saving the outbox
 * entity) lives in the shared {@link OutboxRecorder} (see
 * {@code outbox-support} module, and {@code docs/decisions.md} for the
 * extraction writeup). This class's own job is purely payment-service
 * specific: building the right Avro event object from the data
 * {@link PaymentCommandService} has on hand after processing a
 * payment/refund attempt — same shape as inventory-service's
 * {@code OutboxWriter}.
 */
@Component
public class OutboxWriter {

    private final OutboxRecorder outboxRecorder;

    public OutboxWriter(OutboxRecorder outboxRecorder) {
        this.outboxRecorder = outboxRecorder;
    }

    public void writePaymentApproved(UUID orderId, String paymentId) {
        PaymentApproved event = new PaymentApproved(
                UUID.randomUUID().toString(), orderId.toString(), paymentId, Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, event);
    }

    public void writePaymentDeclined(UUID orderId, String reason) {
        PaymentDeclined event = new PaymentDeclined(
                UUID.randomUUID().toString(), orderId.toString(), reason, Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, event);
    }

    public void writePaymentRefunded(UUID orderId) {
        PaymentRefunded event = new PaymentRefunded(
                UUID.randomUUID().toString(), orderId.toString(), Instant.now());
        outboxRecorder.record(orderId, KafkaTopics.PAYMENT_EVENTS, event);
    }
}
