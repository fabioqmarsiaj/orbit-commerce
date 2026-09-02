package com.fabioqmarsiaj.inventory.api;

import com.fabioqmarsiaj.inventory.persistence.StockEntity;

public record StockResponse(
        String productId,
        int quantityAvailable,
        int quantityReserved
) {

    public static StockResponse from(StockEntity entity) {
        return new StockResponse(entity.getProductId(), entity.getQuantityAvailable(), entity.getQuantityReserved());
    }
}
