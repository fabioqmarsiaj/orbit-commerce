package com.fabioqmarsiaj.order.messaging;

/**
 * Names of every Kafka topic order-service either publishes to or consumes
 * from, matching the topics created by {@code infra/kafka/init-topics.sh}.
 */
public final class KafkaTopics {

    /** Published: OrderCreated / OrderCompleted / OrderCancelled / OrderFailed. */
    public static final String ORDER_EVENTS = "order.events";

    /** Published: ReserveStockCommand / ReleaseStockCommand. */
    public static final String INVENTORY_COMMANDS = "inventory.commands";

    /** Consumed: StockReserved / StockRejected / StockReleased. */
    public static final String INVENTORY_EVENTS = "inventory.events";

    /** Published: ProcessPaymentCommand / RefundPaymentCommand. */
    public static final String PAYMENT_COMMANDS = "payment.commands";

    /** Consumed: PaymentApproved / PaymentDeclined / PaymentRefunded. */
    public static final String PAYMENT_EVENTS = "payment.events";

    /** Published: CreateShipmentCommand. */
    public static final String SHIPPING_COMMANDS = "shipping.commands";

    /** Consumed: ShipmentCreated / ShipmentFailed. */
    public static final String SHIPPING_EVENTS = "shipping.events";

    private KafkaTopics() {
    }
}
