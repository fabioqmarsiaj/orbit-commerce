package com.fabioqmarsiaj.payment.messaging;

/**
 * Names of every Kafka topic payment-service either publishes to or
 * consumes from, matching the topics created by
 * {@code infra/kafka/init-topics.sh}.
 */
public final class KafkaTopics {

    /** Consumed: ProcessPaymentCommand / RefundPaymentCommand. */
    public static final String PAYMENT_COMMANDS = "payment.commands";

    /** Published: PaymentApproved / PaymentDeclined / PaymentRefunded. */
    public static final String PAYMENT_EVENTS = "payment.events";

    private KafkaTopics() {
    }
}
