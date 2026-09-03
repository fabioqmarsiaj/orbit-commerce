package com.fabioqmarsiaj.outbox.application;

import com.fabioqmarsiaj.outbox.persistence.OutboxEntity;
import com.fabioqmarsiaj.outbox.persistence.OutboxRepository;
import com.fabioqmarsiaj.outbox.persistence.OutboxStatus;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Turns one Avro {@link SpecificRecord} into a {@link OutboxEntity} row and
 * saves it, as {@link OutboxStatus#PENDING}.
 *
 * <p>Extracted from what used to be near-identical private helper methods
 * inside order-service's and inventory-service's own {@code OutboxWriter}
 * classes — each service still has its own thin {@code OutboxWriter} that
 * knows how to build the specific Avro record(s) it needs to publish
 * (order-service translates a list of domain events raised by its
 * event-sourced {@code Order} aggregate; inventory-service builds
 * {@code StockReserved}/{@code StockRejected}/{@code StockReleased}
 * directly from a reservation attempt's outcome) and then delegates the
 * mechanical "encode + build entity + save" part to this class.
 *
 * <p><b>Important:</b> do NOT use a generic Jackson {@code ObjectMapper} to
 * serialize the Avro {@link SpecificRecord} here. Avro-generated classes
 * expose a {@code getSchema()} bean-style getter returning
 * {@code org.apache.avro.Schema}, a complex object Jackson's default bean
 * introspection cannot cleanly serialize/deserialize. Instead, we use
 * Avro's own JSON encoding support: {@link EncoderFactory#jsonEncoder} with
 * a {@link SpecificDatumWriter}, writing into a
 * {@link ByteArrayOutputStream}. This is the same encoding
 * {@code AbstractOutboxPublisher} reverses with
 * {@code DecoderFactory#jsonDecoder} + {@code SpecificDatumReader}.
 *
 * <p>Must be called within the same transaction as the domain state
 * change it's recording the "intent to publish" for — this component
 * doesn't manage transactions itself, it just participates in whatever
 * transaction is already active on the calling thread (typical Spring
 * behavior for a plain {@code @Component} with no {@code @Transactional}
 * of its own).
 */
@Component
public class OutboxRecorder {

    private final OutboxRepository repository;

    public OutboxRecorder(OutboxRepository repository) {
        this.repository = repository;
    }

    /**
     * Encodes {@code avroRecord} and writes one {@link OutboxEntity} row
     * for it, keyed by {@code aggregateId} (used as the Kafka partition
     * key later, by the publisher) and {@code topic}. The row's
     * {@code eventType} is the Avro class's simple name (e.g.
     * {@code "OrderCreated"}), which the corresponding
     * {@code AbstractOutboxPublisher} subclass's {@code toAvroRecord} must
     * know how to map back to the right class.
     */
    public void record(UUID aggregateId, String topic, SpecificRecord avroRecord) {
        String json = toAvroJson(avroRecord);

        OutboxEntity outboxEntity = new OutboxEntity(
                UUID.randomUUID(),
                aggregateId,
                topic,
                avroRecord.getClass().getSimpleName(),
                json,
                OutboxStatus.PENDING,
                Instant.now()
        );
        repository.save(outboxEntity);
    }

    /**
     * Encodes an Avro {@link SpecificRecord} as a JSON string using Avro's
     * own schema-aware JSON codec (see class javadoc for why this can't be
     * plain Jackson).
     */
    @SuppressWarnings("unchecked")
    private String toAvroJson(SpecificRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
            writer.write(record, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to Avro-JSON encode " + record.getClass().getSimpleName(), e);
        }
    }
}
