package com.fabioqmarsiaj.query.api;

import com.fabioqmarsiaj.query.streams.TopProductsQueryService;

public record TopProductResponse(
        String productId,
        long viewCount
) {

    public static TopProductResponse from(TopProductsQueryService.ProductViewCount count) {
        return new TopProductResponse(count.productId(), count.viewCount());
    }
}
