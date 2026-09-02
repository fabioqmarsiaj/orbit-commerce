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
        List<OrderLineItem> items = request.items().stream()
                .map(i -> new OrderLineItem(i.productId(), i.quantity(), i.unitPriceCents()))
                .toList();

        UUID orderId = commandService.createOrder(request.customerId(), items);

        return ResponseEntity.status(HttpStatus.CREATED).body(new CreateOrderResponse(orderId));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable("id") UUID id) {
        Order order = eventStore.load(id);
        if (order == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(OrderResponse.from(order));
    }
}
