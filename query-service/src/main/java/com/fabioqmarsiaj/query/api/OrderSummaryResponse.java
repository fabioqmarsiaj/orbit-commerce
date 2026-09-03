package com.fabioqmarsiaj.query.api;

import com.fabioqmarsiaj.query.persistence.OrderSummaryEntity;
import com.fabioqmarsiaj.query.persistence.OrderSummaryStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderSummaryResponse(
        UUID orderId,
        String customerId,
        long totalAmountCents,
        OrderSummaryStatus status,
        Instant createdAt,
        Instant updatedAt
) {

    public static OrderSummaryResponse from(OrderSummaryEntity entity) {
        return new OrderSummaryResponse(entity.getOrderId(), entity.getCustomerId(), entity.getTotalAmountCents(),
                entity.getStatus(), entity.getCreatedAt(), entity.getUpdatedAt());
    }
}
