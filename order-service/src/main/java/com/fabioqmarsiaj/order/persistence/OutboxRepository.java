package com.fabioqmarsiaj.order.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEntity, UUID> {

    /**
     * @return pending outbox rows, oldest first, so the poller publishes
     *         events in the order they were recorded. Consider adding a
     *         {@code Pageable}/limit parameter later to avoid pulling an
     *         unbounded number of rows in one poll cycle.
     */
    // TODO: declare a derived query method, e.g.:
    //  List<OutboxEntity> findByStatusOrderByCreatedAtAsc(OutboxStatus status);
}
