package com.fabioqmarsiaj.query.messaging;

/**
 * Names of every Kafka topic query-service consumes from, matching the
 * topics created by {@code infra/kafka/init-topics.sh}.
 *
 * <p>Unlike every other service in this project, query-service never
 * publishes — there is no corresponding "published" topic constant here,
 * and the {@code *.commands} topics (Saga-orchestration-only, between
 * order-service and the participant services) are intentionally absent
 * too, since query-service only cares about the four {@code *.events}
 * topics for the read model, plus {@code user-activity.events} for the
 * Kafka Streams analytics topology (Part B — see
 * {@code UserActivityStreamsConfig}).
 */
public final class KafkaTopics {

    /** Consumed: OrderCreated / OrderCompleted / OrderCancelled / OrderFailed. */
    public static final String ORDER_EVENTS = "order.events";

    /** Consumed: StockReserved / StockRejected / StockReleased. */
    public static final String INVENTORY_EVENTS = "inventory.events";

    /** Consumed: PaymentApproved / PaymentDeclined / PaymentRefunded. */
    public static final String PAYMENT_EVENTS = "payment.events";

    /** Consumed: ShipmentCreated / ShipmentFailed. */
    public static final String SHIPPING_EVENTS = "shipping.events";

    /** Consumed (Part B, Kafka Streams): ProductViewed / AddedToCart / SearchPerformed. */
    public static final String USER_ACTIVITY_EVENTS = "user-activity.events";

    private KafkaTopics() {
    }
}
