package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.inventory.StockRejected;
import com.fabioqmarsiaj.events.inventory.StockReleased;
import com.fabioqmarsiaj.events.inventory.StockReserved;
import com.fabioqmarsiaj.inventory.persistence.OutboxEntity;
import com.fabioqmarsiaj.inventory.persistence.OutboxRepository;
import com.fabioqmarsiaj.inventory.persistence.OutboxStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;

/**
 * The out-of-band poller that completes the Outbox pattern for
 * inventory-service: periodically reads {@link OutboxStatus#PENDING} rows
 * and actually publishes them to Kafka, then marks them
 * {@link OutboxStatus#PUBLISHED}.
 *
 * <p>Structurally identical to {@code order-service}'s {@code OutboxPublisher}
 * — see that class's Javadoc for the full reasoning (why each row is
 * published in its own transaction rather than one transaction for the
 * whole batch, why the send is synchronous via {@code .join()}, why a
 * failed send leaves the row {@code PENDING} for retry instead of losing
 * it). The only difference is the set of Avro types this service knows
 * how to decode.
 */
@Slf4j
@Component
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate,
                            PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}")
    public void publishPending() {
        List<OutboxEntity> pending = repository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (OutboxEntity row : pending) {
            try {
                transactionTemplate.executeWithoutResult(status -> publishRow(row));
            } catch (Exception e) {
                log.error("Outbox: failed to publish {} for order {} to topic {} - will retry next poll",
                        row.getEventType(), row.getAggregateId(), row.getTopic(), e);
            }
        }
    }

    private void publishRow(OutboxEntity row) {
        SpecificRecord record = toAvroRecord(row.getEventType(), row.getPayload());
        kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), record).join();
        row.markPublished(Instant.now());
        repository.save(row);
        log.info("Outbox: published {} for order {} to topic {}",
                row.getEventType(), row.getAggregateId(), row.getTopic());
    }

    private SpecificRecord toAvroRecord(String eventType, String payload) {
        return switch (eventType) {
            case "StockReserved" -> decode(payload, StockReserved.getClassSchema(), new SpecificDatumReader<StockReserved>(StockReserved.getClassSchema()));
            case "StockRejected" -> decode(payload, StockRejected.getClassSchema(), new SpecificDatumReader<StockRejected>(StockRejected.getClassSchema()));
            case "StockReleased" -> decode(payload, StockReleased.getClassSchema(), new SpecificDatumReader<StockReleased>(StockReleased.getClassSchema()));
            default -> throw new IllegalArgumentException("Unknown outbox event type: " + eventType);
        };
    }

    private <T extends SpecificRecord> T decode(String payload, org.apache.avro.Schema schema,
                                                 SpecificDatumReader<T> reader) {
        try {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payload);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to Avro-JSON decode outbox payload", e);
        }
    }
}
