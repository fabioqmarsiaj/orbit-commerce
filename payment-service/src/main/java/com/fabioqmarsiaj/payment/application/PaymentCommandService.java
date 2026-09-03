package com.fabioqmarsiaj.payment.application;

import com.fabioqmarsiaj.events.payment.ProcessPaymentCommand;
import com.fabioqmarsiaj.events.payment.RefundPaymentCommand;
import com.fabioqmarsiaj.payment.persistence.PaymentEntity;
import com.fabioqmarsiaj.payment.persistence.PaymentRepository;
import com.fabioqmarsiaj.payment.persistence.PaymentStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * Reacts to Saga commands received on {@code payment.commands}
 * (see {@code PaymentCommandListener}), the payment-service equivalent
 * of {@code order-service}'s {@code OrderCommandService} /
 * inventory-service's {@code InventoryCommandService}.
 *
 * <p>Each handler here does the payment/refund processing AND writes the
 * resulting outbox row in the SAME transaction — this is what makes the
 * Outbox pattern's atomicity guarantee hold: whatever
 * {@link PaymentRepository} actually persisted and whatever
 * {@link OutboxWriter} recorded as "intent to publish" either both commit
 * together, or neither does.
 */
@Slf4j
@Service
public class PaymentCommandService {

    private final PaymentRepository paymentRepository;
    private final OutboxWriter outboxWriter;
    private final long approvalLimitCents;

    public PaymentCommandService(PaymentRepository paymentRepository, OutboxWriter outboxWriter,
                                  @Value("${payment.approval.limit-cents}") long approvalLimitCents) {
        this.paymentRepository = paymentRepository;
        this.outboxWriter = outboxWriter;
        this.approvalLimitCents = approvalLimitCents;
    }

    /**
     * Simulates payment processing with a single, deliberately simple
     * rule: amounts above the configured limit are declined, everything
     * else is approved. Persists the outcome (see {@link PaymentEntity} —
     * needed later to validate/process a corresponding
     * {@code RefundPaymentCommand}, since that command doesn't carry back
     * the {@code paymentId} generated here) and records the matching
     * outbox event.
     */
    @Transactional
    public void handleProcessPayment(ProcessPaymentCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());
        long amountCents = command.getAmountCents();

        if (amountCents > approvalLimitCents) {
            paymentRepository.save(new PaymentEntity(orderId, null, amountCents, PaymentStatus.DECLINED));
            String reason = "Amount " + amountCents + " exceeds approval limit " + approvalLimitCents;
            outboxWriter.writePaymentDeclined(orderId, reason);
            log.info("Order {}: payment declined ({}), recorded to outbox", orderId, reason);
        } else {
            String paymentId = UUID.randomUUID().toString();
            paymentRepository.save(new PaymentEntity(orderId, paymentId, amountCents, PaymentStatus.APPROVED));
            outboxWriter.writePaymentApproved(orderId, paymentId);
            log.info("Order {}: payment approved (paymentId={}), recorded to outbox", orderId, paymentId);
        }
    }

    /**
     * Processes a refund compensation. Looks up the previously approved
     * payment by {@code orderId} (see {@link PaymentEntity}'s Javadoc for
     * why this lookup is necessary rather than derivable from the command
     * alone). If no approved payment is found — defensively handled, not
     * expected to happen given the Saga's design, but Kafka only
     * guarantees at-least-once delivery so a duplicate or
     * out-of-order redelivery isn't impossible — logs a warning and does
     * NOT publish a {@code PaymentRefunded} event, rather than publishing
     * a refund confirmation for a payment that was never actually taken
     * (or was already refunded).
     */
    @Transactional
    public void handleRefundPayment(RefundPaymentCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());

        Optional<PaymentEntity> payment = paymentRepository.findByOrderId(orderId);
        if (payment.isEmpty() || payment.get().getStatus() != PaymentStatus.APPROVED) {
            log.warn("Order {}: received RefundPaymentCommand but no APPROVED payment found (status={}) - ignoring",
                    orderId, payment.map(p -> p.getStatus().toString()).orElse("none"));
            return;
        }

        payment.get().markRefunded();
        outboxWriter.writePaymentRefunded(orderId);
        log.info("Order {}: payment refunded, recorded to outbox", orderId);
    }
}
