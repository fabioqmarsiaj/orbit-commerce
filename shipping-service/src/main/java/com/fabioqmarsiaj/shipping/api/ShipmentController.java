package com.fabioqmarsiaj.shipping.api;

import com.fabioqmarsiaj.shipping.persistence.ShipmentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Read-only API for manually inspecting a shipment's status during
 * testing — same role as payment-service's {@code PaymentController}.
 * shipping-service has no HTTP-triggered write operations; a shipment
 * only ever comes into existence in reaction to a
 * {@code CreateShipmentCommand}.
 */
@RestController
@RequestMapping("/shipments")
public class ShipmentController {

    private final ShipmentRepository shipmentRepository;

    public ShipmentController(ShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<ShipmentResponse> getShipment(@PathVariable("orderId") UUID orderId) {
        return shipmentRepository.findByOrderId(orderId)
                .map(entity -> ResponseEntity.ok(ShipmentResponse.from(entity)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
