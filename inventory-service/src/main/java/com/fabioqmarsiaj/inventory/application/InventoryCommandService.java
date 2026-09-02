package com.fabioqmarsiaj.inventory.application;

import com.fabioqmarsiaj.events.inventory.ReleaseStockCommand;
import com.fabioqmarsiaj.events.inventory.ReserveStockCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * Reacts to Saga commands received on {@code inventory.commands}
 * (see {@code InventoryCommandListener}), the inventory-service equivalent
 * of {@code order-service}'s {@code OrderCommandService}.
 *
 * <p>Each handler here does the reservation/release attempt AND writes the
 * resulting outbox row in the SAME transaction — this is what makes the
 * Outbox pattern's atomicity guarantee hold: whatever
 * {@link StockService} actually did to the {@code stock} table and
 * whatever {@link OutboxWriter} recorded as "intent to publish" either
 * both commit together, or neither does.
 */
@Slf4j
@Service
public class InventoryCommandService {

    private final StockService stockService;
    private final OutboxWriter outboxWriter;

    public InventoryCommandService(StockService stockService, OutboxWriter outboxWriter) {
        this.stockService = stockService;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public void handleReserveStock(ReserveStockCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());

        Optional<String> rejection = stockService.tryReserveAll(orderId, command.getItems());

        if (rejection.isPresent()) {
            outboxWriter.writeStockRejected(orderId, rejection.get());
            log.info("Order {}: stock rejected ({}), recorded to outbox", orderId, rejection.get());
        } else {
            outboxWriter.writeStockReserved(orderId);
            log.info("Order {}: stock reserved for {} item(s), recorded to outbox",
                    orderId, command.getItems().size());
        }
    }

    @Transactional
    public void handleReleaseStock(ReleaseStockCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());

        stockService.releaseAll(orderId, command.getItems());
        outboxWriter.writeStockReleased(orderId);
        log.info("Order {}: stock released for {} item(s), recorded to outbox",
                orderId, command.getItems().size());
    }
}
