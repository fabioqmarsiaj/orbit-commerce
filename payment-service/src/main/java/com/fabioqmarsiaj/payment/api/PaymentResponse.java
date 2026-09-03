package com.fabioqmarsiaj.payment.api;

import com.fabioqmarsiaj.payment.persistence.PaymentEntity;
import com.fabioqmarsiaj.payment.persistence.PaymentStatus;

import java.util.UUID;

public record PaymentResponse(
        UUID orderId,
        String paymentId,
        long amountCents,
        PaymentStatus status
) {

    public static PaymentResponse from(PaymentEntity entity) {
        return new PaymentResponse(entity.getOrderId(), entity.getPaymentId(),
                entity.getAmountCents(), entity.getStatus());
    }
}
