package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.events.order.OrderCancelled;
import com.fabioqmarsiaj.events.order.OrderCompleted;
import com.fabioqmarsiaj.events.order.OrderCreated;
import com.fabioqmarsiaj.events.order.OrderFailed;
import com.fabioqmarsiaj.order.persistence.OutboxEntity;
import com.fabioqmarsiaj.order.persistence.OutboxRepository;
import com.fabioqmarsiaj.order.persistence.OutboxStatus;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;

/**
 * The out-of-band poller that completes the Outbox pattern: periodically
 * reads {@link OutboxStatus#PENDING} rows and actually publishes them to
 * Kafka, then marks them {@link OutboxStatus#PUBLISHED}.
 *
 * <p>This decouples "recording the intent to publish" (done transactionally
 * alongside the domain write, in {@code OrderCommandService}) from
 * "actually publishing" (done here, asynchronously, with retryable
 * failure handling). If Kafka is temporarily unavailable, rows simply stay
 * {@code PENDING} and get picked up on the next poll — nothing is lost.
 *
 * <p>Deserializing the stored payload back into a concrete Avro
 * {@link SpecificRecord} requires knowing its class, which is why
 * {@link OutboxEntity#getEventType()} was stored — see
 * {@link #toAvroRecord} for how {@code eventType} maps back to a Java
 * {@code Class}.
 */
@Component
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Runs on a fixed delay (configure via {@code application.properties},
     * e.g. {@code outbox.publisher.fixed-delay-ms}). Reads all pending
     * rows, publishes each to Kafka (keyed by {@code aggregateId} so all
     * messages for the same order stay ordered on the same partition), and
     * marks them published.
     *
     * <p>The send is performed synchronously (blocking on the returned
     * {@code CompletableFuture} via {@code .join()}) so that a row is only
     * marked {@code PUBLISHED} — and the transaction only committed — after
     * Kafka has actually acknowledged it. If the send fails, the exception
     * propagates, the transaction rolls back, and the row stays
     * {@code PENDING} for the next poll to retry — this is what gives us
     * at-least-once delivery instead of silently losing events on a
     * transient Kafka error.
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}")
    @Transactional
    public void publishPending() {
        List<OutboxEntity> pending = repository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (OutboxEntity row : pending) {
            SpecificRecord record = toAvroRecord(row.getEventType(), row.getPayload());
            kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), record).join();
            row.markPublished(Instant.now());
            repository.save(row);
        }
    }

    /**
     * Reconstructs the exact Avro {@link SpecificRecord} instance from its
     * stored (eventType, Avro-JSON payload) pair, mirroring the encoding
     * {@code OutboxWriter} used to write it.
     */
    private SpecificRecord toAvroRecord(String eventType, String payload) {
        return switch (eventType) {
            case "OrderCreated" -> decode(payload, OrderCreated.getClassSchema(), new SpecificDatumReader<OrderCreated>(OrderCreated.getClassSchema()));
            case "OrderCompleted" -> decode(payload, OrderCompleted.getClassSchema(), new SpecificDatumReader<OrderCompleted>(OrderCompleted.getClassSchema()));
            case "OrderCancelled" -> decode(payload, OrderCancelled.getClassSchema(), new SpecificDatumReader<OrderCancelled>(OrderCancelled.getClassSchema()));
            case "OrderFailed" -> decode(payload, OrderFailed.getClassSchema(), new SpecificDatumReader<OrderFailed>(OrderFailed.getClassSchema()));
            default -> throw new IllegalArgumentException("Unknown outbox event type: " + eventType);
        };
    }

    /**
     * Decodes an Avro-JSON payload back into a {@link SpecificRecord}
     * using Avro's schema-aware JSON codec — the exact reverse of
     * {@code OutboxWriter#toAvroJson}.
     */
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
