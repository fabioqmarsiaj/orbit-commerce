package com.fabioqmarsiaj.inventory.messaging;

import com.fabioqmarsiaj.events.inventory.ReleaseStockCommand;
import com.fabioqmarsiaj.events.inventory.ReserveStockCommand;
import com.fabioqmarsiaj.inventory.application.InventoryCommandService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code inventory.commands}, reacting to both Saga command
 * types that flow through it: {@link ReserveStockCommand} (initial
 * reservation attempt) and {@link ReleaseStockCommand} (compensation).
 *
 * <p>Same class-level {@code @KafkaListener} + method-level
 * {@code @KafkaHandler} pattern used by {@code order-service}'s listeners
 * (e.g. {@code InventoryEventListener}) — see that class's Javadoc for why:
 * it lets Spring Kafka route different deserialized Avro types arriving on
 * the same topic to different handler methods, based on runtime type.
 */
@Component
@KafkaListener(topics = KafkaTopics.INVENTORY_COMMANDS, groupId = "inventory-service")
public class InventoryCommandListener {

    private final InventoryCommandService commandService;

    public InventoryCommandListener(InventoryCommandService commandService) {
        this.commandService = commandService;
    }

    @KafkaHandler
    public void onReserveStock(ReserveStockCommand command) {
        commandService.handleReserveStock(command);
    }

    @KafkaHandler
    public void onReleaseStock(ReleaseStockCommand command) {
        commandService.handleReleaseStock(command);
    }
}
