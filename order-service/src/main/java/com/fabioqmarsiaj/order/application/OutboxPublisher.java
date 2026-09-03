package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.events.inventory.ReleaseStockCommand;
import com.fabioqmarsiaj.events.inventory.ReserveStockCommand;
import com.fabioqmarsiaj.events.order.OrderCancelled;
import com.fabioqmarsiaj.events.order.OrderCompleted;
import com.fabioqmarsiaj.events.order.OrderCreated;
import com.fabioqmarsiaj.events.order.OrderFailed;
import com.fabioqmarsiaj.events.payment.ProcessPaymentCommand;
import com.fabioqmarsiaj.events.payment.RefundPaymentCommand;
import com.fabioqmarsiaj.events.shipping.CreateShipmentCommand;
import com.fabioqmarsiaj.outbox.application.AbstractOutboxPublisher;
import com.fabioqmarsiaj.outbox.persistence.OutboxRepository;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * order-service's concrete outbox publisher — see
 * {@link AbstractOutboxPublisher} (in the shared {@code outbox-support}
 * module) for the full polling/transaction/retry mechanics, which this
 * class inherits unchanged. The only order-service-specific piece is
 * {@link #toAvroRecord}: mapping a stored {@code eventType} string back to
 * the concrete Avro class it needs to be decoded into.
 *
 * <p>Handles two different kinds of outgoing messages through the exact
 * same mechanism: the four {@code Order*} integration events (published
 * to {@code order.events}, written by {@code OutboxWriter#writeAll}) AND
 * the five Saga commands order-service sends to participant services
 * (published to {@code inventory.commands}/{@code payment.commands}/
 * {@code shipping.commands}, written by {@code OutboxWriter#writeCommand}).
 * Both share the same {@code outbox} table and the same publish loop —
 * the {@code topic} column (already present on every row) is what
 * determines where each one actually gets sent; {@code toAvroRecord} only
 * needs to know how to decode each {@code eventType}, not which topic it
 * came from. See {@code docs/decisions.md} for why commands were moved
 * into the outbox after initially being sent directly via
 * {@code KafkaTemplate}.
 */
@Component
public class OutboxPublisher extends AbstractOutboxPublisher {

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate,
                            PlatformTransactionManager transactionManager) {
        super(repository, kafkaTemplate, transactionManager);
    }

    /**
     * Declared here (not in the base class) so the
     * {@code fixedDelayString} placeholder is resolved against
     * order-service's own {@code application.properties}, and so
     * {@code @Scheduled} is unambiguously on this concrete bean's method.
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}")
    @Override
    public void publishPending() {
        super.publishPending();
    }

    @Override
    protected SpecificRecord toAvroRecord(String eventType, String payload) {
        return switch (eventType) {
            case "OrderCreated" -> decode(payload, OrderCreated.getClassSchema(), new SpecificDatumReader<OrderCreated>(OrderCreated.getClassSchema()));
            case "OrderCompleted" -> decode(payload, OrderCompleted.getClassSchema(), new SpecificDatumReader<OrderCompleted>(OrderCompleted.getClassSchema()));
            case "OrderCancelled" -> decode(payload, OrderCancelled.getClassSchema(), new SpecificDatumReader<OrderCancelled>(OrderCancelled.getClassSchema()));
            case "OrderFailed" -> decode(payload, OrderFailed.getClassSchema(), new SpecificDatumReader<OrderFailed>(OrderFailed.getClassSchema()));
            case "ReserveStockCommand" -> decode(payload, ReserveStockCommand.getClassSchema(), new SpecificDatumReader<ReserveStockCommand>(ReserveStockCommand.getClassSchema()));
            case "ReleaseStockCommand" -> decode(payload, ReleaseStockCommand.getClassSchema(), new SpecificDatumReader<ReleaseStockCommand>(ReleaseStockCommand.getClassSchema()));
            case "ProcessPaymentCommand" -> decode(payload, ProcessPaymentCommand.getClassSchema(), new SpecificDatumReader<ProcessPaymentCommand>(ProcessPaymentCommand.getClassSchema()));
            case "RefundPaymentCommand" -> decode(payload, RefundPaymentCommand.getClassSchema(), new SpecificDatumReader<RefundPaymentCommand>(RefundPaymentCommand.getClassSchema()));
            case "CreateShipmentCommand" -> decode(payload, CreateShipmentCommand.getClassSchema(), new SpecificDatumReader<CreateShipmentCommand>(CreateShipmentCommand.getClassSchema()));
            default -> throw new IllegalArgumentException("Unknown outbox event type: " + eventType);
        };
    }
}
