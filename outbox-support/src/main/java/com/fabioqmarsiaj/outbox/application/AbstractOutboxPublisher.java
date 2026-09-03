package com.fabioqmarsiaj.outbox.application;

import com.fabioqmarsiaj.outbox.persistence.OutboxEntity;
import com.fabioqmarsiaj.outbox.persistence.OutboxRepository;
import com.fabioqmarsiaj.outbox.persistence.OutboxStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
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
 * <p>Extracted from what used to be two near-identical {@code OutboxPublisher}
 * classes in order-service and inventory-service — see
 * {@code docs/decisions.md} for the extraction writeup. Each service keeps
 * a small concrete {@code @Component OutboxPublisher extends
 * AbstractOutboxPublisher}, which:
 * <ul>
 *   <li>Declares its own {@code @Scheduled} method that just calls
 *       {@code super.publishPending()} — kept in the subclass (not moved
 *       here) so the {@code fixedDelayString} property placeholder is
 *       resolved against each service's own {@code application.properties},
 *       and so {@code @Scheduled} is unambiguously on a concrete,
 *       Spring-proxied bean method rather than relying on annotation
 *       inheritance behavior.</li>
 *   <li>Implements {@link #toAvroRecord}, the one part that's genuinely
 *       specific to each service: the switch from a stored
 *       {@code eventType} string back to the concrete Avro class it needs
 *       to be decoded into.</li>
 * </ul>
 *
 * <p>This decouples "recording the intent to publish" (done transactionally
 * alongside the domain write, via {@link OutboxRecorder}) from "actually
 * publishing" (done here, asynchronously, with retryable failure
 * handling). If Kafka is temporarily unavailable, rows simply stay
 * {@code PENDING} and get picked up on the next poll — nothing is lost.
 *
 * <p>Deliberately NOT one big {@code @Transactional} method spanning the
 * whole batch of pending rows: each row is published inside its OWN
 * transaction (see {@link #publishRow}, run via a programmatic
 * {@link TransactionTemplate}). If publishing were wrapped in a single
 * transaction spanning the whole loop, one failing row (e.g. a Schema
 * Registry compatibility error — see {@code docs/decisions.md} Phase 3) would
 * roll back the DB changes for every row already successfully published
 * earlier in the SAME loop iteration — but their Kafka sends already
 * happened and can't be un-sent, so those rows would revert to
 * {@code PENDING} and get needlessly re-published (a duplicate) on the
 * next poll. Per-row transactions confine both the success and the
 * failure to just that one row, and a failure for one row doesn't stop the
 * rest of the batch from being attempted in the same poll.
 *
 * <p>A {@link TransactionTemplate} (rather than the declarative
 * {@code @Transactional} annotation) is used specifically because this is
 * a plain method-to-method call within the same class
 * ({@link #publishPending} calling {@link #publishRow}) — an annotated
 * {@code @Transactional} on {@code publishRow} would be silently ignored
 * due to Spring AOP's "self-invocation" limitation (calling an annotated
 * method via {@code this.x()} bypasses the dynamic proxy entirely).
 * {@code TransactionTemplate} starts a real transaction directly, with no
 * proxy involved, sidestepping that pitfall.
 *
 * <p>Caveat: this assumes a single instance of the poller runs at a time.
 * If a service is ever scaled to multiple replicas, two instances could
 * pick up and publish the same {@code PENDING} row in the same poll
 * window (each still individually safe/at-least-once, but more
 * duplicate-prone) — a {@code SELECT ... FOR UPDATE SKIP LOCKED} style
 * read would be the fix, not implemented here. See
 * {@code docs/decisions.md}.
 */
@Slf4j
public abstract class AbstractOutboxPublisher {

    private final OutboxRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;

    protected AbstractOutboxPublisher(OutboxRepository repository, KafkaTemplate<String, Object> kafkaTemplate,
                                       PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Reads all pending rows and attempts to publish each one, keyed by
     * {@code aggregateId} so all messages for the same aggregate (e.g. the
     * same order) stay ordered on the same partition. Meant to be called
     * from a subclass's {@code @Scheduled} method.
     */
    public void publishPending() {
        List<OutboxEntity> pending = repository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (OutboxEntity row : pending) {
            try {
                transactionTemplate.executeWithoutResult(status -> publishRow(row));
            } catch (Exception e) {
                log.error("Outbox: failed to publish {} for aggregate {} to topic {} - will retry next poll",
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
        log.info("Outbox: published {} for aggregate {} to topic {}",
                row.getEventType(), row.getAggregateId(), row.getTopic());
    }

    /**
     * Reconstructs the exact Avro {@link SpecificRecord} instance from its
     * stored (eventType, Avro-JSON payload) pair, mirroring the encoding
     * {@link OutboxRecorder} used to write it. Each service's concrete
     * subclass implements this as a {@code switch} over the event type
     * names it's responsible for publishing.
     */
    protected abstract SpecificRecord toAvroRecord(String eventType, String payload);

    /**
     * Decodes an Avro-JSON payload back into a {@link SpecificRecord}
     * using Avro's schema-aware JSON codec — the exact reverse of
     * {@link OutboxRecorder}'s encoding. A shared helper for subclasses'
     * {@link #toAvroRecord} implementations to call, one branch per event
     * type.
     */
    protected final <T extends SpecificRecord> T decode(String payload, Schema schema, SpecificDatumReader<T> reader) {
        try {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, payload);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to Avro-JSON decode outbox payload", e);
        }
    }
}
