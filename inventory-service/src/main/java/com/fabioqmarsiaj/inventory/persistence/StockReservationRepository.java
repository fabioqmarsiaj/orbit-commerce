package com.fabioqmarsiaj.inventory.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StockReservationRepository extends JpaRepository<StockReservationEntity, UUID> {

    List<StockReservationEntity> findByOrderIdAndStatus(UUID orderId, StockReservationStatus status);
}
