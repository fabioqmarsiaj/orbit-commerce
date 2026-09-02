package com.fabioqmarsiaj.order.messaging;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.order.application.OrderCommandService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code inventory.events}, reacting only to the two message
 * types that matter to the Saga: {@link StockReserved} and
 * {@link StockRejected}. (A third possible message on this topic,
 * {@code StockReleased}, is a compensation ack that order-service does not
 * need to react to — the order has already moved to CANCELLED by the time
 * it arrives.)
 */
@Component
public class InventoryEventListener {

    private final OrderCommandService commandService;

    public InventoryEventListener(OrderCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "order-service")
    public void onStockReserved(StockReserved event) {
        // TODO: parse event.getOrderId() (String) into a UUID, then call
        //  commandService.handleStockReserved(orderId).
        //
        // Note: since inventory.events carries more than one message type,
        // think about how @KafkaListener should be configured to route
        // each Avro type to the right handler method (hint: look into
        // Spring Kafka's @KafkaHandler + a class-level @KafkaListener, as
        // an alternative to one @KafkaListener method per type like this
        // skeleton currently assumes — pick whichever approach you
        // understand best and we'll discuss trade-offs in review).
        throw new UnsupportedOperationException("not implemented yet");
    }

    public void onStockRejected(StockRejected event) {
        // TODO: parse event.getOrderId(), then call
        //  commandService.handleStockRejected(orderId, event.getReason()).
        throw new UnsupportedOperationException("not implemented yet");
    }
}
