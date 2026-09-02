package com.fabioqmarsiaj.order.api;

import com.fabioqmarsiaj.order.application.OrderCommandService;
import com.fabioqmarsiaj.order.application.OrderEventStore;
import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderCommandService commandService;
    private final OrderEventStore eventStore;

    public OrderController(OrderCommandService commandService, OrderEventStore eventStore) {
        this.commandService = commandService;
        this.eventStore = eventStore;
    }

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        // TODO:
        //  1. Map request.items() (List<CreateOrderRequest.LineItemRequest>)
        //     into List<OrderLineItem> (the domain value object).
        //  2. UUID orderId = commandService.createOrder(request.customerId(), items);
        //  3. Return ResponseEntity.status(HttpStatus.CREATED).body(new CreateOrderResponse(orderId));
        throw new UnsupportedOperationException("not implemented yet");
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable("id") UUID id) {
        // TODO:
        //  1. Order order = eventStore.load(id);
        //  2. If order is null (no events found for this id), return
        //     ResponseEntity.notFound().build().
        //  3. Otherwise return ResponseEntity.ok(OrderResponse.from(order)).
        throw new UnsupportedOperationException("not implemented yet");
    }
}
