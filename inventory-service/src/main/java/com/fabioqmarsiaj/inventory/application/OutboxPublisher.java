package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.outbox.application.AbstractOutboxPublisher;
import com.fabioqmarsiaj.outbox.persistence.OutboxRepository;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * inventory-service's concrete outbox publisher — see
 * {@link AbstractOutboxPublisher} (in the shared {@code outbox-support}
 * module) for the full polling/transaction/retry mechanics, which this
 * class inherits unchanged. The only inventory-service-specific piece is
 * {@link #toAvroRecord}: mapping a stored {@code eventType} string back to
 * the concrete Avro class it needs to be decoded into.
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
     * inventory-service's own {@code application.properties}, and so
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
            case "StockReserved" -> decode(payload, StockReserved.getClassSchema(), new SpecificDatumReader<StockReserved>(StockReserved.getClassSchema()));
            case "StockRejected" -> decode(payload, StockRejected.getClassSchema(), new SpecificDatumReader<StockRejected>(StockRejected.getClassSchema()));
            case "StockReleased" -> decode(payload, StockReleased.getClassSchema(), new SpecificDatumReader<StockReleased>(StockReleased.getClassSchema()));
            default -> throw new IllegalArgumentException("Unknown outbox event type: " + eventType);
        };
    }
}
