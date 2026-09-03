package com.fabioqmarsiaj.query.messaging;

import com.fabioqmarsiaj.events.shipping.ShipmentCreated;
import com.fabioqmarsiaj.events.shipping.ShipmentFailed;
import com.fabioqmarsiaj.query.application.TimelineRecorder;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Listens to {@code shipping.events}, feeding only
 * {@link TimelineRecorder} — see {@code InventoryEventListener}'s
 * Javadoc for why this topic never updates {@code OrderSummaryEntity}.
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — see order-service's
 * {@code InventoryEventListener} Javadoc for the full explanation of why.
 * Both types this topic carries are handled below.
 */
@Component
@KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS, groupId = "query-service")
public class ShippingEventListener {

    private final TimelineRecorder timelineRecorder;

    public ShippingEventListener(TimelineRecorder timelineRecorder) {
        this.timelineRecorder = timelineRecorder;
    }

    @KafkaHandler
    public void onShipmentCreated(ShipmentCreated event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.SHIPPING_EVENTS, "ShipmentCreated", Map.of(
                "shipmentId", event.getShipmentId()
        ), event.getOccurredAt());
    }

    @KafkaHandler
    public void onShipmentFailed(ShipmentFailed event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.SHIPPING_EVENTS, "ShipmentFailed", Map.of(
                "reason", event.getReason()
        ), event.getOccurredAt());
    }
}
