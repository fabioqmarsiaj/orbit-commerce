package com.fabioqmarsiaj.query.api;

import com.fabioqmarsiaj.query.persistence.TimelineEntryEntity;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;

public record TimelineEntryResponse(
        String sourceTopic,
        String eventType,
        Map<String, Object> fields,
        Instant occurredAt
) {

    @SuppressWarnings("unchecked")
    public static TimelineEntryResponse from(TimelineEntryEntity entity, ObjectMapper objectMapper) {
        Map<String, Object> fields = objectMapper.readValue(entity.getPayload(), Map.class);
        return new TimelineEntryResponse(entity.getSourceTopic(), entity.getEventType(), fields, entity.getOccurredAt());
    }
}
