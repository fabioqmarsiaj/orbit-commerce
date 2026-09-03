package com.fabioqmarsiaj.shipping.api;

import com.fabioqmarsiaj.shipping.persistence.ShipmentEntity;
import com.fabioqmarsiaj.shipping.persistence.ShipmentStatus;

import java.util.UUID;

public record ShipmentResponse(
        UUID orderId,
        String shipmentId,
        ShipmentStatus status,
        String reason
) {

    public static ShipmentResponse from(ShipmentEntity entity) {
        return new ShipmentResponse(entity.getOrderId(), entity.getShipmentId(),
                entity.getStatus(), entity.getReason());
    }
}
