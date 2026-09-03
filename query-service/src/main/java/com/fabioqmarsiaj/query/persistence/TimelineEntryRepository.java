package com.fabioqmarsiaj.query.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TimelineEntryRepository extends JpaRepository<TimelineEntryEntity, UUID> {

    List<TimelineEntryEntity> findByOrderIdOrderByOccurredAtAsc(UUID orderId);
}
