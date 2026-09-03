package com.fabioqmarsiaj.payment.application;

import com.fabioqmarsiaj.events.payment.PaymentApproved;
import com.fabioqmarsiaj.events.payment.PaymentDeclined;
import com.fabioqmarsiaj.events.payment.PaymentRefunded;
import com.fabioqmarsiaj.outbox.application.AbstractOutboxPublisher;
import com.fabioqmarsiaj.outbox.persistence.OutboxRepository;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * payment-service's concrete outbox publisher — see
 * {@link AbstractOutboxPublisher} (in the shared {@code outbox-support}
 * module) for the full polling/transaction/retry mechanics, which this
 * class inherits unchanged. The only payment-service-specific piece is
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
     * payment-service's own {@code application.properties}, and so
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
            case "PaymentApproved" -> decode(payload, PaymentApproved.getClassSchema(), new SpecificDatumReader<PaymentApproved>(PaymentApproved.getClassSchema()));
            case "PaymentDeclined" -> decode(payload, PaymentDeclined.getClassSchema(), new SpecificDatumReader<PaymentDeclined>(PaymentDeclined.getClassSchema()));
            case "PaymentRefunded" -> decode(payload, PaymentRefunded.getClassSchema(), new SpecificDatumReader<PaymentRefunded>(PaymentRefunded.getClassSchema()));
            default -> throw new IllegalArgumentException("Unknown outbox event type: " + eventType);
        };
    }
}
