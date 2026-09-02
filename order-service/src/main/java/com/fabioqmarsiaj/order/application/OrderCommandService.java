package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.SagaCommandFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * The Saga orchestrator. This is the single place that:
 * <ol>
 *   <li>Loads/creates an {@code Order} aggregate.</li>
 *   <li>Invokes a command method on it, causing it to raise domain
 *       event(s).</li>
 *   <li>Persists those events to the event store + outbox, atomically, in
 *       one DB transaction ({@link OrderEventStore#append} +
 *       {@link OutboxWriter#writeAll}).</li>
 *   <li>After the transaction commits, sends the next Saga command (if
 *       any) to the appropriate participant service, via
 *       {@link SagaCommandFactory} + a Kafka producer.</li>
 * </ol>
 *
 * <p>Step 4 deliberately happens via a direct Kafka send rather than
 * through the outbox — worth reflecting on, and discussing in review,
 * whether that is a meaningful inconsistency in this design (what happens
 * if the process crashes between steps 3 and 4?), and what it would take
 * to fix it (hint: the outbox could carry commands too, with a second
 * poller/topic mapping, or a single generic outbox keyed by topic name —
 * look at how {@link OutboxWriter} already stores an explicit
 * {@code topic} column).
 *
 * <p>Each {@code handleXxx} method corresponds to reacting to one incoming
 * Kafka event from a participant service (see the {@code messaging}
 * package's listeners), except {@link #createOrder}, which handles the
 * initial {@code POST /orders} API call.
 */
@Slf4j
@Service
public class OrderCommandService {

    private final OrderEventStore eventStore;
    private final OutboxWriter outboxWriter;
    private final SagaCommandFactory commandFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderCommandService(OrderEventStore eventStore, OutboxWriter outboxWriter,
                                SagaCommandFactory commandFactory,
                                KafkaTemplate<String, Object> kafkaTemplate) {
        this.eventStore = eventStore;
        this.outboxWriter = outboxWriter;
        this.commandFactory = commandFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Handles {@code POST /orders}: creates a new order and kicks off the
     * Saga by asking inventory-service to reserve stock.
     *
     * @return the id of the newly created order
     */
    @Transactional
    public UUID createOrder(String customerId, List<OrderLineItem> items) {
        UUID orderId = UUID.randomUUID();
        Order order = Order.create(orderId, customerId, items);
        persistAndPullEvents(order);

        kafkaTemplate.send(KafkaTopics.INVENTORY_COMMANDS, orderId.toString(),
                commandFactory.reserveStock(order));
        log.info("Order {}: sent ReserveStockCommand to topic {}", orderId, KafkaTopics.INVENTORY_COMMANDS);

        return orderId;
    }

    /**
     * Reacts to inventory-service confirming stock reservation: advances
     * the order and asks payment-service to process payment.
     */
    @Transactional
    public void handleStockReserved(UUID orderId) {
        Order order = eventStore.load(orderId);
        order.markStockReserved();
        persistAndPullEvents(order);

        kafkaTemplate.send(KafkaTopics.PAYMENT_COMMANDS, orderId.toString(),
                commandFactory.processPayment(order));
        log.info("Order {}: sent ProcessPaymentCommand to topic {}", orderId, KafkaTopics.PAYMENT_COMMANDS);
    }

    /**
     * Reacts to inventory-service rejecting stock reservation: cancels the
     * order. No further commands are needed (nothing to compensate yet,
     * since payment was never attempted).
     */
    @Transactional
    public void handleStockRejected(UUID orderId, String reason) {
        Order order = eventStore.load(orderId);
        order.markStockRejected(reason);
        persistAndPullEvents(order);
        // No outgoing command — the Saga ends here for this order.
    }

    /**
     * Reacts to payment-service approving payment: advances the order and
     * asks shipping-service to create a shipment.
     */
    @Transactional
    public void handlePaymentApproved(UUID orderId, String paymentId) {
        Order order = eventStore.load(orderId);
        order.markPaymentApproved(paymentId);
        persistAndPullEvents(order);

        kafkaTemplate.send(KafkaTopics.SHIPPING_COMMANDS, orderId.toString(),
                commandFactory.createShipment(order));
        log.info("Order {}: sent CreateShipmentCommand to topic {}", orderId, KafkaTopics.SHIPPING_COMMANDS);
    }

    /**
     * Reacts to payment-service declining payment: cancels the order AND
     * compensates by releasing the previously reserved stock.
     */
    @Transactional
    public void handlePaymentDeclined(UUID orderId, String reason) {
        Order order = eventStore.load(orderId);
        order.markPaymentDeclined(reason);
        persistAndPullEvents(order);

        kafkaTemplate.send(KafkaTopics.INVENTORY_COMMANDS, orderId.toString(),
                commandFactory.releaseStock(order));
        log.info("Order {}: sent ReleaseStockCommand (compensation) to topic {}", orderId, KafkaTopics.INVENTORY_COMMANDS);
    }

    /**
     * Reacts to shipping-service confirming shipment creation: the happy
     * path terminal transition, order is now COMPLETED. No further
     * commands needed.
     */
    @Transactional
    public void handleShipmentCreated(UUID orderId, String shipmentId) {
        Order order = eventStore.load(orderId);
        order.markShipmentCreated(shipmentId);
        persistAndPullEvents(order);
    }

    /**
     * Reacts to shipping-service failing to create a shipment: the final
     * compensation path. The order fails AND we must both refund the
     * payment and release the reserved stock.
     */
    @Transactional
    public void handleShipmentFailed(UUID orderId, String reason) {
        Order order = eventStore.load(orderId);
        order.markShipmentFailed(reason);
        persistAndPullEvents(order);

        kafkaTemplate.send(KafkaTopics.PAYMENT_COMMANDS, orderId.toString(),
                commandFactory.refundPayment(order));
        log.info("Order {}: sent RefundPaymentCommand (compensation) to topic {}", orderId, KafkaTopics.PAYMENT_COMMANDS);

        kafkaTemplate.send(KafkaTopics.INVENTORY_COMMANDS, orderId.toString(),
                commandFactory.releaseStock(order));
        log.info("Order {}: sent ReleaseStockCommand (compensation) to topic {}", orderId, KafkaTopics.INVENTORY_COMMANDS);
    }

    /**
     * Shared helper: pulls whatever domain events the aggregate just
     * raised, and persists them to both the event store and the outbox.
     * Every {@code handleXxx}/{@code createOrder} method above calls this
     * exactly once, right after invoking a command method on the
     * aggregate.
     */
    private void persistAndPullEvents(Order order) {
        var events = order.pullPendingEvents();
        eventStore.append(order.getId(), events);
        outboxWriter.writeAll(order.getId(), events);
        log.info("Order {}: persisted {} event(s) to order_events and outbox", order.getId(), events.size());
    }
}
