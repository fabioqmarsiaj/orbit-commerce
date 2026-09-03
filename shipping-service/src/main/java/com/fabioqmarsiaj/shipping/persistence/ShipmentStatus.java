package com.fabioqmarsiaj.shipping.persistence;

/**
 * Status of a {@link ShipmentEntity} row.
 */
public enum ShipmentStatus {
    /** Shipment was successfully created. */
    CREATED,
    /** Shipment creation failed (simulated). */
    FAILED
}
