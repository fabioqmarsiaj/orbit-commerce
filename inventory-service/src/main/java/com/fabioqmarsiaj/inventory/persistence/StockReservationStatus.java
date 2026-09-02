package com.fabioqmarsiaj.inventory.persistence;

/**
 * Status of a {@link StockReservationEntity} row.
 */
public enum StockReservationStatus {
    /** Stock was successfully reserved for this (order, product) pair. */
    RESERVED,
    /** Stock was later released (compensation via ReleaseStockCommand). */
    RELEASED
}
