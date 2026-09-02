package com.fabioqmarsiaj.order.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * Repository for the append-only event store. Note there are intentionally
 * no update/delete query methods here — only inserts (via {@code save})
 * and ordered reads.
 */
public interface OrderEventRepository extends JpaRepository<OrderEventEntity, UUID> {

    /**
     * Returns every event recorded for the given order, in the exact order
     * they occurred (by {@code sequenceNumber}). This is the query used both
     * to rehydrate an {@code Order} aggregate, and (via
     * {@code List::size}) to compute the next sequence number when
     * appending new events — see {@code OrderEventStore}.
     */
    List<OrderEventEntity> findByOrderIdOrderBySequenceNumberAsc(UUID orderId);
}
