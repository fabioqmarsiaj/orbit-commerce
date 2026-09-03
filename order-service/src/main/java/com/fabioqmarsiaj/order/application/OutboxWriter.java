package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.event.OrderDomainEvent;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.OrderEventTranslator;
import com.fabioqmarsiaj.outbox.application.OutboxRecorder;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Turns publishable domain events into outbox rows.
 *
 * <p>The mechanical part of this (Avro-JSON encoding + building/saving the
 * outbox entity) lives in the shared {@link OutboxRecorder} (see
 * {@code outbox-support} module, and {@code docs/decisions.md} for the
 * extraction writeup). This class's own job is purely order-service
 * specific: translating each raised {@link OrderDomainEvent} into its
 * Avro-mapped equivalent (via {@link OrderEventTranslator#toAvro}) and
 * filtering out events that aren't meant to be published at all
 * ({@link OrderEventTranslator#isPublishable}).
 *
 * <p>Must be called within the same transaction as
 * {@code OrderEventStore#append} for the Outbox pattern's atomicity
 * guarantee to hold.
 */
@Component
public class OutboxWriter {

    private final OutboxRecorder outboxRecorder;
    private final OrderEventTranslator translator;

    public OutboxWriter(OutboxRecorder outboxRecorder, OrderEventTranslator translator) {
        this.outboxRecorder = outboxRecorder;
        this.translator = translator;
    }

    /**
     * Filters {@code events} down to the publishable ones and records one
     * outbox row per event, all destined for {@link KafkaTopics#ORDER_EVENTS}.
     */
    public void writeAll(UUID orderId, List<OrderDomainEvent> events) {
        for (OrderDomainEvent event : events) {
            if (!translator.isPublishable(event)) {
                continue;
            }

            SpecificRecord avroRecord = translator.toAvro(event);
            outboxRecorder.record(orderId, KafkaTopics.ORDER_EVENTS, avroRecord);
        }
    }

    /**
     * Records one outbox row for a Saga command that {@code OrderCommandService}
     * needs to send to a participant service (inventory-service,
     * payment-service, or shipping-service), on whichever
     * {@code *.commands} topic the caller specifies.
     *
     * <p>Saga commands used to be sent directly via {@code KafkaTemplate},
     * bypassing the outbox entirely — a real gap, since
     * {@code KafkaTemplate#send} returns a {@code CompletableFuture} that
     * was never awaited/observed: an async send failure (e.g. a broker
     * outage) would be silently swallowed after the enclosing transaction
     * had already committed, leaving the order stuck in its current state
     * forever with no record of the failure. Routing commands through the
     * outbox (this method) closes that gap the exact same way it's already
     * closed for {@code order.events}: the "intent to send" is now
     * recorded in the SAME transaction as the domain event(s) that
     * triggered it, and {@code AbstractOutboxPublisher}'s synchronous
     * send + per-row transaction + automatic retry-on-next-poll take over
     * from there. See {@code docs/decisions.md} for the full writeup.
     */
    public void writeCommand(UUID orderId, String topic, SpecificRecord command) {
        outboxRecorder.record(orderId, topic, command);
    }
}
