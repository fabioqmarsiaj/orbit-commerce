package com.fabioqmarsiaj.inventory.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEntity, UUID> {

    /**
     * @return pending outbox rows, oldest first, so the poller publishes
     *         events in the order they were recorded.
     */
    List<OutboxEntity> findByStatusOrderByCreatedAtAsc(OutboxStatus status);
}
