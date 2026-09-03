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
}
