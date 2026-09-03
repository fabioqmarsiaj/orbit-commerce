package com.fabioqmarsiaj.query.api;

import com.fabioqmarsiaj.query.streams.TopProductsQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Interactive query endpoint over the Kafka Streams topology in
 * {@code UserActivityStreamsConfig} (Phase 7, Part B).
 *
 * <p>Note: until ingestion-service (Phase 8) exists to actually produce
 * {@code ProductViewed} events onto {@code user-activity.events}, this
 * endpoint will always return an empty list — there's simply no traffic
 * for the topology to count yet. This is expected, not a bug; see
 * {@code docs/decisions.md} ("Phase 7 — query-service, Part B").
 */
@RestController
@RequestMapping("/analytics")
public class AnalyticsController {

    private static final int DEFAULT_LIMIT = 10;

    private final TopProductsQueryService topProductsQueryService;

    public AnalyticsController(TopProductsQueryService topProductsQueryService) {
        this.topProductsQueryService = topProductsQueryService;
    }

    @GetMapping("/top-products")
    public List<TopProductResponse> getTopProducts(
            @RequestParam(value = "limit", required = false, defaultValue = "" + DEFAULT_LIMIT) int limit) {
        return topProductsQueryService.topProducts(limit).stream()
                .map(TopProductResponse::from)
                .toList();
    }
}
