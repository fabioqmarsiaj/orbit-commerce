package com.fabioqmarsiaj.payment.api;

import com.fabioqmarsiaj.payment.persistence.PaymentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Read-only API for manually inspecting a payment's status during testing
 * — payment-service otherwise has no HTTP-triggered write operations,
 * unlike inventory-service's {@code POST /stock} seed endpoint: there's
 * nothing analogous to "seed" here, since a payment only ever comes into
 * existence in reaction to a {@code ProcessPaymentCommand}.
 */
@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentRepository paymentRepository;

    public PaymentController(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable("orderId") UUID orderId) {
        return paymentRepository.findByOrderId(orderId)
                .map(entity -> ResponseEntity.ok(PaymentResponse.from(entity)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
