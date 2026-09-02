package com.fabioqmarsiaj.inventory.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface StockRepository extends JpaRepository<StockEntity, String> {

    Optional<StockEntity> findByProductId(String productId);

    /**
     * Atomically decrements {@code quantity_available} and increments
     * {@code quantity_reserved} for one product, but ONLY if enough stock
     * is available — the {@code AND quantity_available >= :quantity} guard
     * makes this a single conditional UPDATE instead of a
     * read-then-check-then-write sequence, which would otherwise be
     * vulnerable to a race between two concurrent reservations for the
     * same product (Saga commands are Kafka-partitioned by {@code orderId},
     * not {@code productId}, so two different orders reserving the same
     * product CAN be processed concurrently by different consumer threads).
     *
     * <p>Returns the number of rows updated: {@code 1} if the reservation
     * succeeded, {@code 0} if there wasn't enough stock (or the product
     * doesn't exist) — the caller checks this return value rather than
     * relying on an exception. See {@code docs/decisions.md} for the full
     * reasoning ("atomic conditional UPDATE" pattern).
     */
    @Modifying
    @Query("""
            UPDATE StockEntity s
            SET s.quantityAvailable = s.quantityAvailable - :quantity,
                s.quantityReserved = s.quantityReserved + :quantity,
                s.updatedAt = CURRENT_TIMESTAMP
            WHERE s.productId = :productId AND s.quantityAvailable >= :quantity
            """)
    int tryReserve(@Param("productId") String productId, @Param("quantity") int quantity);

    /**
     * The reverse of {@link #tryReserve}: gives stock back. Used both by
     * the {@code ReleaseStockCommand} handler (compensation) and, within
     * the same transaction, to undo any items already reserved for an
     * order when a later item in the same command can't be reserved (the
     * "all or nothing" rule — see {@code StockService}).
     *
     * <p>No conditional guard is needed here: releasing stock can never
     * fail due to insufficient stock, only decrement (reservation) can.
     */
    @Modifying
    @Query("""
            UPDATE StockEntity s
            SET s.quantityAvailable = s.quantityAvailable + :quantity,
                s.quantityReserved = s.quantityReserved - :quantity,
                s.updatedAt = CURRENT_TIMESTAMP
            WHERE s.productId = :productId
            """)
    void release(@Param("productId") String productId, @Param("quantity") int quantity);
}
