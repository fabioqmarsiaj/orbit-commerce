package com.fabioqmarsiaj.query.messaging;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.query.application.TimelineRecorder;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Listens to {@code inventory.events}, feeding only
 * {@link TimelineRecorder} (unlike {@code OrderEventListener}, this
 * topic never updates {@code OrderSummaryEntity} — see that entity's
 * Javadoc).
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — see order-service's
 * {@code InventoryEventListener} Javadoc for the full explanation of why.
 * All three types this topic carries are handled below.
 */
@Component
@KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "query-service")
public class InventoryEventListener {

    private final TimelineRecorder timelineRecorder;

    public InventoryEventListener(TimelineRecorder timelineRecorder) {
        this.timelineRecorder = timelineRecorder;
    }

    @KafkaHandler
    public void onStockReserved(StockReserved event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, "StockReserved", Map.of(), event.getOccurredAt());
    }

    @KafkaHandler
    public void onStockRejected(StockRejected event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, "StockRejected", Map.of(
                "reason", event.getReason()
        ), event.getOccurredAt());
    }

    @KafkaHandler
    public void onStockReleased(StockReleased event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.INVENTORY_EVENTS, "StockReleased", Map.of(), event.getOccurredAt());
    }
}
