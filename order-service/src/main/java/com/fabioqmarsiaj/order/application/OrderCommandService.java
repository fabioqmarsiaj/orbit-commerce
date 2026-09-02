package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.SagaCommandFactory;
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
        // TODO:
        //  1. UUID orderId = UUID.randomUUID();
        //  2. Order order = Order.create(orderId, customerId, items);
        //  3. persistAndPullEvents(order) — see helper below.
        //  4. Send commandFactory.reserveStock(order) to
        //     KafkaTopics.INVENTORY_COMMANDS via kafkaTemplate (think about
        //     what key to use — the order id, as a String, is a sensible
        //     partition key so all messages for one order land on the same
        //     partition and stay ordered).
        //  5. Return orderId.
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to inventory-service confirming stock reservation: advances
     * the order and asks payment-service to process payment.
     */
    @Transactional
    public void handleStockReserved(UUID orderId) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markStockReserved();
        //  3. persistAndPullEvents(order);
        //  4. Send commandFactory.processPayment(order) to
        //     KafkaTopics.PAYMENT_COMMANDS.
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to inventory-service rejecting stock reservation: cancels the
     * order. No further commands are needed (nothing to compensate yet,
     * since payment was never attempted).
     */
    @Transactional
    public void handleStockRejected(UUID orderId, String reason) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markStockRejected(reason);
        //  3. persistAndPullEvents(order);
        //  (no outgoing command — the Saga ends here for this order)
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to payment-service approving payment: advances the order and
     * asks shipping-service to create a shipment.
     */
    @Transactional
    public void handlePaymentApproved(UUID orderId, String paymentId) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markPaymentApproved(paymentId);
        //  3. persistAndPullEvents(order);
        //  4. Send commandFactory.createShipment(order) to
        //     KafkaTopics.SHIPPING_COMMANDS.
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to payment-service declining payment: cancels the order AND
     * compensates by releasing the previously reserved stock.
     */
    @Transactional
    public void handlePaymentDeclined(UUID orderId, String reason) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markPaymentDeclined(reason);
        //  3. persistAndPullEvents(order);
        //  4. Send commandFactory.releaseStock(order) to
        //     KafkaTopics.INVENTORY_COMMANDS (compensation).
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to shipping-service confirming shipment creation: the happy
     * path terminal transition, order is now COMPLETED. No further
     * commands needed.
     */
    @Transactional
    public void handleShipmentCreated(UUID orderId, String shipmentId) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markShipmentCreated(shipmentId);
        //  3. persistAndPullEvents(order);
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Reacts to shipping-service failing to create a shipment: the final
     * compensation path. The order fails AND we must both refund the
     * payment and release the reserved stock.
     */
    @Transactional
    public void handleShipmentFailed(UUID orderId, String reason) {
        // TODO:
        //  1. Order order = eventStore.load(orderId);
        //  2. order.markShipmentFailed(reason);
        //  3. persistAndPullEvents(order);
        //  4. Send commandFactory.refundPayment(order) to
        //     KafkaTopics.PAYMENT_COMMANDS (compensation).
        //  5. Send commandFactory.releaseStock(order) to
        //     KafkaTopics.INVENTORY_COMMANDS (compensation).
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Shared helper: pulls whatever domain events the aggregate just
     * raised, and persists them to both the event store and the outbox.
     * Every {@code handleXxx}/{@code createOrder} method above should call
     * this exactly once, right after invoking a command method on the
     * aggregate.
     */
    private void persistAndPullEvents(Order order) {
        // TODO:
        //  1. var events = order.pullPendingEvents();
        //  2. eventStore.append(order.getId(), events);
        //  3. outboxWriter.writeAll(order.getId(), events);
        throw new UnsupportedOperationException("not implemented yet");
    }
}
