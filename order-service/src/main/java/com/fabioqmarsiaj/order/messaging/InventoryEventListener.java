package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Listens to {@code inventory.events}, reacting to all three message
 * types published there: {@link StockReserved}/{@link StockRejected}
 * (Saga-driving) and {@link StockReleased} (a compensation ack — the
 * order has already moved to {@code CANCELLED}/{@code FAILED} by the time
 * it arrives, so there's nothing further to drive forward, only worth
 * logging).
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — Spring Kafka's class-level
 * {@code @KafkaListener} + method-level {@code @KafkaHandler} combination
 * does NOT silently skip a deserialized message with no matching handler.
 * It throws {@code KafkaException: No method found for class ...}, which
 * (with no custom error handler configured) causes the container's
 * default retry-with-backoff behavior to keep re-seeking to the SAME
 * offset and re-delivering the same message forever once backoff attempts
 * are exhausted — effectively wedging the whole partition for this
 * consumer group, not just dropping the one unhandled message. This was
 * discovered live in Phase 4 testing: {@link StockReleased} had never
 * actually been published before (compensation had never been exercised
 * end-to-end), so this class had shipped since Phase 2 with a Javadoc
 * comment incorrectly claiming unhandled types are "simply not
 * delivered." See {@code docs/decisions.md} for the full writeup.
 *
 * <p>This class uses the class-level {@code @KafkaListener} +
 * method-level {@code @KafkaHandler} combination, which is Spring Kafka's
 * idiomatic way to route different message types arriving on the same
 * topic to different handler methods: the container inspects the runtime
 * type of each deserialized Avro record and dispatches it to the
 * {@code @KafkaHandler} method whose single parameter matches that type.
 */
@Slf4j
@Component
@KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "order-service")
public class InventoryEventListener {

    private final OrderCommandService commandService;

    public InventoryEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onStockReserved(StockReserved event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handleStockReserved(orderId);
    }

    @KafkaHandler
    public void onStockRejected(StockRejected event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        commandService.handleStockRejected(orderId, event.getReason());
    }

    /**
     * Compensation acknowledgment — the Saga doesn't advance any further
     * in reaction to this; the order is already in a terminal state
     * ({@code CANCELLED}/{@code FAILED}) by the time stock has been
     * released. Consumed purely so the partition offset advances instead
     * of wedging (see class Javadoc).
     */
    @KafkaHandler
    public void onStockReleased(StockReleased event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        log.info("Order {}: stock released (compensation ack) - Saga ends here for this branch", orderId);
    }
}
