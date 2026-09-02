package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.persistence.OutboxEntity;
import com.fabioqmarsiaj.order.persistence.OutboxRepository;
import com.fabioqmarsiaj.order.persistence.OutboxStatus;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}")
    @Transactional
    public void publishPending() {
        // TODO:
        //  1. List<OutboxEntity> pending = repository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
        //     (implement that derived query method on OutboxRepository if
        //     you haven't already, per its own TODO).
        //  2. For each row:
        //     a. SpecificRecord record = toAvroRecord(row.getEventType(), row.getPayload());
        //     b. kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), record);
        //        (consider: should this be synchronous — .get() the Future
        //        and only mark PUBLISHED on success — or fire-and-forget?
        //        Think through what happens to at-least-once delivery in
        //        each case, we'll discuss in review.)
        //     c. row.markPublished(Instant.now());
        //     d. repository.save(row);
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reconstructs the exact Avro {@link SpecificRecord} instance from its
     * stored (eventType, Avro-JSON payload) pair, mirroring the encoding
     * {@code OutboxWriter} used to write it.
     *
     * <p>Use {@link org.apache.avro.io.DecoderFactory#jsonDecoder} with a
     * {@link org.apache.avro.specific.SpecificDatumReader} configured with
     * the right generated class's {@code SCHEMA$}, symmetrical to how
     * {@code OutboxWriter} is expected to have encoded it.
     */
    private SpecificRecord toAvroRecord(String eventType, String payload) {
        // TODO: implement a switch on eventType covering every Avro type
        //  OrderEventTranslator#toAvro can produce (OrderCreated,
        //  OrderCompleted, OrderCancelled, OrderFailed from the
        //  com.fabioqmarsiaj.events.order package), decoding payload via
        //  the Avro JSON decoder described above.
        throw new UnsupportedOperationException("not implemented yet");
    }
}
