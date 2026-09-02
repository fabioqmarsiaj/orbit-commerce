package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.order.OrderLineItem;
import com.fabioqmarsiaj.inventory.persistence.StockEntity;
import com.fabioqmarsiaj.inventory.persistence.StockRepository;
import com.fabioqmarsiaj.inventory.persistence.StockReservationEntity;
import com.fabioqmarsiaj.inventory.persistence.StockReservationRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Owns the actual stock-level bookkeeping: reserving, releasing, and
 * seeding. Everything here is plain, synchronous, imperative code against
 * {@link StockRepository} — no event-sourced aggregate, unlike
 * {@code order-service}'s {@code Order} (inventory-service doesn't need
 * event replay: "current stock level per product" has no interesting
 * history worth reconstructing, only a current value).
 */
@Service
public class StockService {

    private final StockRepository stockRepository;
    private final StockReservationRepository reservationRepository;

    public StockService(StockRepository stockRepository, StockReservationRepository reservationRepository) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
    }

    /**
     * Creates a new product row, or resets the available quantity of an
     * existing one. Deliberately simple (full reset, not "add N units") —
     * good enough for seeding test data; a real inventory system would
     * separate "receive stock" (increment) from "set stock" (reset).
     */
    public StockEntity seed(String productId, int quantity) {
        return stockRepository.findByProductId(productId)
                .map(existing -> {
                    existing.reseed(quantity);
                    return stockRepository.save(existing);
                })
                .orElseGet(() -> stockRepository.save(new StockEntity(productId, quantity)));
    }

    public Optional<StockEntity> find(String productId) {
        return stockRepository.findByProductId(productId);
    }

    /**
     * Attempts to reserve every line item for one order, atomically as a
     * group: either all items get reserved, or none do ("all or nothing").
     *
     * <p>Items are sorted by {@code productId} first — with multiple
     * products per order and commands partitioned by {@code orderId} (not
     * {@code productId}), two different orders that both touch the same
     * pair of products could otherwise attempt their per-product UPDATEs
     * in opposite order on different threads, which is exactly the
     * classic setup for a database deadlock between two transactions each
     * holding a row lock the other one wants next. Reserving in a fixed
     * (sorted) order across all callers avoids that.
     *
     * <p>Each item is reserved via {@link StockRepository#tryReserve}, a
     * single conditional {@code UPDATE ... WHERE quantity_available >= ?}
     * — see that method's Javadoc for why this is preferred over
     * optimistic locking here. If an item fails (not enough stock, or the
     * product was never seeded), every item already reserved earlier in
     * this same call is rolled back via {@link StockRepository#release}
     * <b>within this same method</b>, rather than relying on the caller's
     * {@code @Transactional} rollback — see {@link InventoryCommandService}
     * for why: the caller needs the reservation attempt's outcome (success
     * or a rejection reason) to decide which outbox event to write, in the
     * SAME transaction, so the transaction as a whole must still commit
     * either way.
     *
     * @return {@code Optional.empty()} if every item was reserved, or the
     *         rejection reason if not (the first item that couldn't be
     *         reserved, by name)
     */
    public Optional<String> tryReserveAll(UUID orderId, List<OrderLineItem> items) {
        List<OrderLineItem> sorted = items.stream()
                .sorted(Comparator.comparing(OrderLineItem::getProductId))
                .toList();

        List<OrderLineItem> reserved = new ArrayList<>();
        for (OrderLineItem item : sorted) {
            String productId = item.getProductId();
            int quantity = item.getQuantity();

            int rowsUpdated = stockRepository.tryReserve(productId, quantity);
            if (rowsUpdated == 0) {
                // Roll back everything reserved so far in this attempt.
                for (OrderLineItem toUndo : reserved) {
                    stockRepository.release(toUndo.getProductId(), toUndo.getQuantity());
                }
                return Optional.of("Insufficient stock for product " + productId);
            }

            reserved.add(item);
        }

        for (OrderLineItem item : reserved) {
            reservationRepository.save(new StockReservationEntity(
                    UUID.randomUUID(), orderId, item.getProductId(), item.getQuantity()));
        }

        return Optional.empty();
    }

    /**
     * Releases every line item for one order (compensation, in reaction to
     * {@code ReleaseStockCommand}). Unlike reservation, release can never
     * fail due to insufficient stock, so there's no "all or nothing"
     * concern here — each item is released independently.
     *
     * <p>Also marks any matching {@code RESERVED} {@link StockReservationEntity}
     * rows as {@code RELEASED}, for audit purposes (see that entity's
     * Javadoc — this project doesn't currently rely on reading these rows
     * back to know what to release; the command already carries that).
     */
    public void releaseAll(UUID orderId, List<OrderLineItem> items) {
        for (OrderLineItem item : items) {
            stockRepository.release(item.getProductId(), item.getQuantity());
        }

        reservationRepository.findByOrderIdAndStatus(orderId, com.fabioqmarsiaj.inventory.persistence.StockReservationStatus.RESERVED)
                .forEach(StockReservationEntity::markReleased);
    }
}
