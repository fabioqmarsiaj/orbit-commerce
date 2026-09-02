package com.fabioqmarsiaj.inventory.messaging;

/**
 * Names of every Kafka topic inventory-service either publishes to or
 * consumes from, matching the topics created by
 * {@code infra/kafka/init-topics.sh}.
 */
public final class KafkaTopics {

    /** Consumed: ReserveStockCommand / ReleaseStockCommand. */
    public static final String INVENTORY_COMMANDS = "inventory.commands";

    /** Published: StockReserved / StockRejected / StockReleased. */
    public static final String INVENTORY_EVENTS = "inventory.events";

    private KafkaTopics() {
    }
}
