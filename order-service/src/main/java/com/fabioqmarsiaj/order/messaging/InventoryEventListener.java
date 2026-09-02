package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Listens to {@code inventory.events}, reacting only to the two message
 * types that matter to the Saga: {@link StockReserved} and
 * {@link StockRejected}. (A third possible message on this topic,
 * {@code StockReleased}, is a compensation ack that order-service does not
 * need to react to — the order has already moved to CANCELLED by the time
 * it arrives.)
 *
 * <p>This class uses the class-level {@code @KafkaListener} +
 * method-level {@code @KafkaHandler} combination, which is Spring Kafka's
 * idiomatic way to route different message types arriving on the same
 * topic to different handler methods: the container inspects the runtime
 * type of each deserialized Avro record and dispatches it to the
 * {@code @KafkaHandler} method whose single parameter matches that type.
 * A message type without a matching handler (e.g. {@code StockReleased})
 * is simply not delivered to this listener.
 */
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
}
