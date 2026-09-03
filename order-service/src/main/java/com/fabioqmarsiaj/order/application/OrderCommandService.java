package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.SagaCommandFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
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
 *   <li>Records the next Saga command (if any) to the outbox too, in the
 *       SAME transaction ({@link OutboxWriter#writeCommand}) — the actual
 *       send to the participant service happens later, out-of-band, via
 *       {@link OutboxPublisher}.</li>
 * </ol>
 *
 * <p>Steps 3 and 4 both go through the outbox now — Saga commands used to
 * be sent directly via a Kafka producer instead, which had a real gap:
 * {@code KafkaTemplate#send} returns a {@code CompletableFuture} that was
 * never awaited, so an asynchronous send failure (e.g. a transient broker
 * outage, as opposed to a synchronous failure like a schema error) was
 * silently swallowed after the transaction had already committed — the
 * order would advance in the database but the command that was supposed
 * to move the Saga forward would simply never arrive, with no error, no
 * retry, and no way to notice short of manually inspecting the database.
 * Routing commands through the outbox closes that gap the same way it was
 * already closed for {@code order.events}: recording the "intent to send"
 * atomically with the domain change, and letting
 * {@link OutboxPublisher}'s synchronous send + per-row transaction +
 * automatic retry-on-next-poll take over from there. See
 * {@code docs/decisions.md} for the full writeup.
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

    public OrderCommandService(OrderEventStore eventStore, OutboxWriter outboxWriter,
                                SagaCommandFactory commandFactory) {
        this.eventStore = eventStore;
        this.outboxWriter = outboxWriter;
        this.commandFactory = commandFactory;
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

        recordCommand(orderId, KafkaTopics.INVENTORY_COMMANDS, commandFactory.reserveStock(order));

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

        recordCommand(orderId, KafkaTopics.PAYMENT_COMMANDS, commandFactory.processPayment(order));
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

        recordCommand(orderId, KafkaTopics.SHIPPING_COMMANDS, commandFactory.createShipment(order));
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

        recordCommand(orderId, KafkaTopics.INVENTORY_COMMANDS, commandFactory.releaseStock(order));
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

        recordCommand(orderId, KafkaTopics.PAYMENT_COMMANDS, commandFactory.refundPayment(order));
        recordCommand(orderId, KafkaTopics.INVENTORY_COMMANDS, commandFactory.releaseStock(order));
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

    /**
     * Shared helper: records one Saga command to the outbox, in the same
     * transaction as whatever domain event(s) triggered it (see
     * {@link #persistAndPullEvents}, always called first). The actual
     * send to Kafka happens later, out-of-band, via {@link OutboxPublisher}.
     */
    private void recordCommand(UUID orderId, String topic, SpecificRecord command) {
        outboxWriter.writeCommand(orderId, topic, command);
        log.info("Order {}: recorded {} command to outbox for topic {}",
                orderId, command.getClass().getSimpleName(), topic);
    }
}
