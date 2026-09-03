package com.fabioqmarsiaj.query.application;

import com.fabioqmarsiaj.query.persistence.TimelineEntryEntity;
import com.fabioqmarsiaj.query.persistence.TimelineEntryRepository;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Records one {@link TimelineEntryEntity} row per domain event consumed
 * by any of the four {@code *EventListener} classes in this service —
 * the shared mechanical piece each listener delegates to, the same
 * "extract the boilerplate into one small component" shape as
 * {@code outbox-support}'s {@code OutboxRecorder} plays for the
 * producer-side services.
 *
 * <p>Note Spring Boot 4 defaults to <b>Jackson 3</b>
 * ({@code tools.jackson.databind.ObjectMapper}), not the classic Jackson 2
 * — see order-service's {@code OrderEventMapper} for the fuller
 * explanation. The {@code fields} map is serialized as plain JSON (not
 * Avro's own JSON codec, unlike every outbox writer in this project) —
 * deliberately: this table is read back only as a generic
 * {@code Map<String,Object>} for the timeline API response (see
 * {@code TimelineQueryService}), never deserialized into a strongly
 * typed Avro {@code SpecificRecord}, so there's no {@code getSchema()}
 * bean-introspection pitfall to sidestep here the way there is when
 * writing to the outbox.
 */
@Component
public class TimelineRecorder {

    private final TimelineEntryRepository repository;
    private final ObjectMapper objectMapper;

    public TimelineRecorder(TimelineEntryRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /**
     * Records one timeline entry.
     *
     * @param orderId     the order this event relates to
     * @param sourceTopic which of the four {@code *.events} topics this
     *                    arrived on (see {@code KafkaTopics})
     * @param eventType   the Avro record's simple class name (e.g.
     *                    {@code "StockReserved"})
     * @param fields      the event's own fields, as a plain map (e.g.
     *                    {@code Map.of("reason", event.getReason())}) —
     *                    deliberately NOT the raw Avro record itself, to
     *                    avoid the {@code getSchema()} Jackson pitfall
     * @param occurredAt  the event's own {@code occurredAt} timestamp
     */
    public void record(UUID orderId, String sourceTopic, String eventType,
                        Map<String, Object> fields, Instant occurredAt) {
        String payload = objectMapper.writeValueAsString(fields);
        repository.save(new TimelineEntryEntity(
                UUID.randomUUID(), orderId, sourceTopic, eventType, payload, occurredAt));
    }
}
