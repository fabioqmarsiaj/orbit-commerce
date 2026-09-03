package com.fabioqmarsiaj.payment.messaging;

import com.fabioqmarsiaj.events.payment.ProcessPaymentCommand;
import com.fabioqmarsiaj.events.payment.RefundPaymentCommand;
import com.fabioqmarsiaj.payment.application.PaymentCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code payment.commands}, reacting to both Saga command
 * types that flow through it: {@link ProcessPaymentCommand} (initial
 * payment attempt) and {@link RefundPaymentCommand} (compensation).
 *
 * <p>Same class-level {@code @KafkaListener} + method-level
 * {@code @KafkaHandler} pattern used throughout this project (e.g.
 * inventory-service's {@code InventoryCommandListener}) — see that
 * class's Javadoc for why: it lets Spring Kafka route different
 * deserialized Avro types arriving on the same topic to different
 * handler methods, based on runtime type.
 */
@Component
@KafkaListener(topics = KafkaTopics.PAYMENT_COMMANDS, groupId = "payment-service")
public class PaymentCommandListener {

    private final PaymentCommandService commandService;

    public PaymentCommandListener(PaymentCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onProcessPayment(ProcessPaymentCommand command) {
        commandService.handleProcessPayment(command);
    }

    @KafkaHandler
    public void onRefundPayment(RefundPaymentCommand command) {
        commandService.handleRefundPayment(command);
    }
}
