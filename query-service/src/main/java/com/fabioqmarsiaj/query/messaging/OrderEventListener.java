package com.fabioqmarsiaj.query.messaging;

import com.fabioqmarsiaj.events.order.OrderCancelled;
import com.fabioqmarsiaj.events.order.OrderCompleted;
import com.fabioqmarsiaj.events.order.OrderCreated;
import com.fabioqmarsiaj.events.order.OrderFailed;
import com.fabioqmarsiaj.query.application.OrderSummaryProjector;
import com.fabioqmarsiaj.query.application.TimelineRecorder;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Listens to {@code order.events}, the only topic
 * {@link OrderSummaryProjector} is fed from (see that class's Javadoc)
 * — also feeds {@link TimelineRecorder} like every other listener in
 * this service.
 *
 * <p><b>Every publishable event type on this topic MUST have a matching
 * {@code @KafkaHandler} here</b> — see order-service's
 * {@code InventoryEventListener} Javadoc for the full explanation of why
 * (an unhandled type throws and wedges the partition, doesn't silently
 * skip). {@code order.events} carries exactly four types, all handled
 * below.
 */
@Component
@KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "query-service")
public class OrderEventListener {

    private final TimelineRecorder timelineRecorder;
    private final OrderSummaryProjector summaryProjector;

    public OrderEventListener(TimelineRecorder timelineRecorder, OrderSummaryProjector summaryProjector) {
        this.timelineRecorder = timelineRecorder;
        this.summaryProjector = summaryProjector;
    }

    @KafkaHandler
    public void onOrderCreated(OrderCreated event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.ORDER_EVENTS, "OrderCreated", Map.of(
                "customerId", event.getCustomerId(),
                "totalAmountCents", event.getTotalAmountCents(),
                "itemCount", event.getItems().size()
        ), event.getOccurredAt());
        summaryProjector.onOrderCreated(orderId, event.getCustomerId(), event.getTotalAmountCents(), event.getOccurredAt());
    }

    @KafkaHandler
    public void onOrderCompleted(OrderCompleted event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.ORDER_EVENTS, "OrderCompleted", Map.of(), event.getOccurredAt());
        summaryProjector.onOrderCompleted(orderId, event.getOccurredAt());
    }

    @KafkaHandler
    public void onOrderCancelled(OrderCancelled event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.ORDER_EVENTS, "OrderCancelled", Map.of(
                "reason", event.getReason()
        ), event.getOccurredAt());
        summaryProjector.onOrderCancelled(orderId, event.getOccurredAt());
    }

    @KafkaHandler
    public void onOrderFailed(OrderFailed event) {
        UUID orderId = UUID.fromString(event.getOrderId());
        timelineRecorder.record(orderId, KafkaTopics.ORDER_EVENTS, "OrderFailed", Map.of(
                "reason", event.getReason()
        ), event.getOccurredAt());
        summaryProjector.onOrderFailed(orderId, event.getOccurredAt());
    }
}
