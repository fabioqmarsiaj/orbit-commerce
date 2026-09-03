package com.fabioqmarsiaj.shipping.messaging;

/**
 * Names of every Kafka topic shipping-service either publishes to or
 * consumes from, matching the topics created by
 * {@code infra/kafka/init-topics.sh}.
 */
public final class KafkaTopics {

    /** Consumed: CreateShipmentCommand. */
    public static final String SHIPPING_COMMANDS = "shipping.commands";

    /** Published: ShipmentCreated / ShipmentFailed. */
    public static final String SHIPPING_EVENTS = "shipping.events";

    private KafkaTopics() {
    }
}
