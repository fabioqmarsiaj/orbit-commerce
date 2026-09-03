package com.fabioqmarsiaj.query.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderSummaryRepository extends JpaRepository<OrderSummaryEntity, UUID> {

    Optional<OrderSummaryEntity> findByOrderId(UUID orderId);

    List<OrderSummaryEntity> findByStatusOrderByCreatedAtDesc(OrderSummaryStatus status);

    List<OrderSummaryEntity> findAllByOrderByCreatedAtDesc();
}
