package com.fabioqmarsiaj.shipping.application;

import com.fabioqmarsiaj.events.shipping.CreateShipmentCommand;
import com.fabioqmarsiaj.shipping.persistence.ShipmentEntity;
import com.fabioqmarsiaj.shipping.persistence.ShipmentRepository;
import com.fabioqmarsiaj.shipping.persistence.ShipmentStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Reacts to Saga commands received on {@code shipping.commands} (see
 * {@code ShippingCommandListener}), the shipping-service equivalent of
 * {@code order-service}'s {@code OrderCommandService} /
 * inventory-service's {@code InventoryCommandService} / payment-service's
 * {@code PaymentCommandService}.
 *
 * <p>Does the shipment-creation attempt AND writes the resulting outbox
 * row in the SAME transaction — this is what makes the Outbox pattern's
 * atomicity guarantee hold: whatever {@link ShipmentRepository} actually
 * persisted and whatever {@link OutboxWriter} recorded as "intent to
 * publish" either both commit together, or neither does.
 */
@Slf4j
@Service
public class ShippingCommandService {

    private final ShipmentRepository shipmentRepository;
    private final OutboxWriter outboxWriter;
    private final String forceFailCustomerId;

    public ShippingCommandService(ShipmentRepository shipmentRepository, OutboxWriter outboxWriter,
                                   @Value("${shipping.simulation.force-fail-customer-id}") String forceFailCustomerId) {
        this.shipmentRepository = shipmentRepository;
        this.outboxWriter = outboxWriter;
        this.forceFailCustomerId = forceFailCustomerId;
    }

    /**
     * Simulates shipment creation. {@link CreateShipmentCommand} carries
     * no business field (amount, items, etc.) to key a realistic
     * success/failure decision on — unlike inventory-service's stock
     * check or payment-service's amount threshold — so this uses a
     * sentinel {@code customerId} instead (see
     * {@code shipping.simulation.force-fail-customer-id} in
     * {@code application.properties}): matching it forces a simulated
     * failure, giving deterministic, per-request control over the
     * compensation path during manual testing. Persists the outcome (see
     * {@link ShipmentEntity} — audit/query only here, nothing downstream
     * depends on reading it back, unlike payment-service's
     * {@code PaymentEntity}) and records the matching outbox event.
     */
    @Transactional
    public void handleCreateShipment(CreateShipmentCommand command) {
        UUID orderId = UUID.fromString(command.getOrderId());
        String customerId = command.getCustomerId();

        if (forceFailCustomerId.equals(customerId)) {
            String reason = "Simulated shipment failure for customer " + customerId;
            shipmentRepository.save(new ShipmentEntity(orderId, null, ShipmentStatus.FAILED, reason));
            outboxWriter.writeShipmentFailed(orderId, reason);
            log.info("Order {}: shipment failed ({}), recorded to outbox", orderId, reason);
        } else {
            String shipmentId = UUID.randomUUID().toString();
            shipmentRepository.save(new ShipmentEntity(orderId, shipmentId, ShipmentStatus.CREATED, null));
            outboxWriter.writeShipmentCreated(orderId, shipmentId);
            log.info("Order {}: shipment created (shipmentId={}), recorded to outbox", orderId, shipmentId);
        }
    }
}
