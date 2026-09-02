package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.events.order.OrderCancelled;
import com.fabioqmarsiaj.events.order.OrderCompleted;
import com.fabioqmarsiaj.events.order.OrderCreated;
import com.fabioqmarsiaj.events.order.OrderFailed;
import com.fabioqmarsiaj.order.persistence.OutboxEntity;
import com.fabioqmarsiaj.order.persistence.OutboxRepository;
import com.fabioqmarsiaj.order.persistence.OutboxStatus;
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

    /**
     * Runs on a fixed delay (configure via {@code application.properties},
     * e.g. {@code outbox.publisher.fixed-delay-ms}). Reads all pending
     * rows and attempts to publish each one, keyed by {@code aggregateId}
     * so all messages for the same order stay ordered on the same
     * partition.
     *
     * <p>Deliberately NOT one big {@code @Transactional} method: each row
     * is published inside its OWN transaction (see {@link #publishRow},
     * run via {@link #transactionTemplate}). If publishing were wrapped in
     * a single transaction spanning the whole loop, one failing row (e.g.
     * a Schema Registry compatibility error) would roll back the DB
     * changes for every row already successfully published earlier in the
     * SAME loop iteration — but their Kafka sends already happened and
     * can't be un-sent, so those rows would revert to {@code PENDING} and
     * get needlessly re-published (a duplicate) on the next poll. Per-row
     * transactions confine both the success and the failure to just that
     * one row, and a failure for one row doesn't stop the rest of the
     * batch from being attempted in the same poll.
     *
     * <p>A row that fails simply stays {@code PENDING} (its transaction
     * never commits) and is retried on the next poll — see
     * {@link #publishRow} for why the send itself is synchronous.
     *
     * <p>Caveat: this assumes a single instance of the poller runs at a
     * time. If this service ever scales to multiple replicas, two
     * instances could pick up and publish the same {@code PENDING} row in
     * the same window (each still individually safe/at-least-once, but
     * more duplicate-prone) — a {@code SELECT ... FOR UPDATE SKIP LOCKED}
     * style read would be the fix, not implemented here. See
     * docs/decisions.md.
     */
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

    /**
     * Publishes exactly one outbox row, within its own transaction (see
     * {@link #publishPending}). The send is performed synchronously
     * (blocking on the returned {@code CompletableFuture} via
     * {@code .join()}) so that the row is only marked {@code PUBLISHED} —
     * and this transaction only committed — after Kafka has actually
     * acknowledged it. If the send throws, this transaction rolls back and
     * the row stays {@code PENDING} — this is what gives us at-least-once
     * delivery instead of silently losing events on a transient Kafka
     * error.
     */
    private void publishRow(OutboxEntity row) {
        SpecificRecord record = toAvroRecord(row.getEventType(), row.getPayload());
        kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), record).join();
        row.markPublished(Instant.now());
        repository.save(row);
        log.info("Outbox: published {} for order {} to topic {}",
                row.getEventType(), row.getAggregateId(), row.getTopic());
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
