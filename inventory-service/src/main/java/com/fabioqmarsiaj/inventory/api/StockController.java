package com.fabioqmarsiaj.inventory.api;

import com.fabioqmarsiaj.inventory.application.StockService;
import com.fabioqmarsiaj.inventory.persistence.StockEntity;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
public class StockController {

    private final StockService stockService;

    public StockController(StockService stockService) {
        this.stockService = stockService;
    }

    /**
     * Seeds (creates or resets) a product's available stock. Not part of
     * the Saga — this is purely a test-data convenience endpoint, the
     * inventory-service equivalent of manually inserting rows via
     * {@code psql} before exercising the reservation flow.
     */
    @PostMapping
    public ResponseEntity<StockResponse> seedStock(@Valid @RequestBody SeedStockRequest request) {
        StockEntity entity = stockService.seed(request.productId(), request.quantity());
        return ResponseEntity.ok(StockResponse.from(entity));
    }

    @GetMapping("/{productId}")
    public ResponseEntity<StockResponse> getStock(@PathVariable("productId") String productId) {
        return stockService.find(productId)
                .map(entity -> ResponseEntity.ok(StockResponse.from(entity)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
