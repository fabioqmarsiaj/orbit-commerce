package com.fabioqmarsiaj.query.api;

import com.fabioqmarsiaj.query.persistence.OrderSummaryRepository;
import com.fabioqmarsiaj.query.persistence.OrderSummaryStatus;
import com.fabioqmarsiaj.query.persistence.TimelineEntryRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-only API over the two read models this service maintains:
 * {@code TimelineEntryEntity} (granular, cross-topic event log per
 * order) and {@code OrderSummaryEntity} (one-row-per-order status,
 * projected only from {@code order.events} — see that entity's
 * Javadoc).
 */
@RestController
@RequestMapping("/orders")
public class OrderQueryController {

    private final TimelineEntryRepository timelineEntryRepository;
    private final OrderSummaryRepository orderSummaryRepository;
    private final ObjectMapper objectMapper;

    public OrderQueryController(TimelineEntryRepository timelineEntryRepository,
                                 OrderSummaryRepository orderSummaryRepository,
                                 ObjectMapper objectMapper) {
        this.timelineEntryRepository = timelineEntryRepository;
        this.orderSummaryRepository = orderSummaryRepository;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/{id}/timeline")
    public List<TimelineEntryResponse> getTimeline(@PathVariable("id") UUID orderId) {
        return timelineEntryRepository.findByOrderIdOrderByOccurredAtAsc(orderId).stream()
                .map(entity -> TimelineEntryResponse.from(entity, objectMapper))
                .toList();
    }

    @GetMapping
    public List<OrderSummaryResponse> getOrders(@RequestParam(value = "status", required = false) OrderSummaryStatus status) {
        List<com.fabioqmarsiaj.query.persistence.OrderSummaryEntity> summaries = Optional.ofNullable(status)
                .map(orderSummaryRepository::findByStatusOrderByCreatedAtDesc)
                .orElseGet(orderSummaryRepository::findAllByOrderByCreatedAtDesc);
        return summaries.stream().map(OrderSummaryResponse::from).toList();
    }
}
