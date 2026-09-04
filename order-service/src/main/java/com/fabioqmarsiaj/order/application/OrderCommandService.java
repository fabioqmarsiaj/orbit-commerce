package com.fabioqmarsiaj.order.application;

import com.fabioqmarsiaj.order.domain.Order;
import com.fabioqmarsiaj.order.domain.OrderLineItem;
import com.fabioqmarsiaj.order.messaging.KafkaTopics;
import com.fabioqmarsiaj.order.messaging.SagaCommandFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
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
 *
 * <p>Since this is the only class in the project with visibility over a
 * Saga's entire lifecycle (start to terminal transition), it's also where
 * the Phase 9 custom Saga metrics live: {@code orbit.saga.duration} (a
 * {@link Timer}, recorded at every terminal transition — see
 * {@link #recordSagaDuration}) and {@code orbit.saga.compensation} (a
 * {@link Counter}, incremented only in the 3 compensation-triggering
 * handlers — see {@link #recordCompensation}). Both deliberately tag with a
 * small, fixed-cardinality discriminator ({@code outcome}/{@code trigger})
 * rather than the triggering event's free-text {@code reason} field, which
 * would create one Prometheus time series per distinct reason string
 * (unbounded cardinality) — see {@code docs/decisions.md} ("Phase 9 —
 * Observability") for the full reasoning.
 */
@Slf4j
@Service
public class OrderCommandService {

    private static final String SAGA_DURATION_METRIC = "orbit.saga.duration";
    private static final String SAGA_COMPENSATION_METRIC = "orbit.saga.compensation";

    private final OrderEventStore eventStore;
    private final OutboxWriter outboxWriter;
    private final SagaCommandFactory commandFactory;
    private final MeterRegistry meterRegistry;

    public OrderCommandService(OrderEventStore eventStore, OutboxWriter outboxWriter,
                                SagaCommandFactory commandFactory, MeterRegistry meterRegistry) {
        this.eventStore = eventStore;
        this.outboxWriter = outboxWriter;
        this.commandFactory = commandFactory;
        this.meterRegistry = meterRegistry;
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

        recordCompensation("STOCK_REJECTED");
        recordSagaDuration(order, "CANCELLED");
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

        recordCompensation("PAYMENT_DECLINED");
        recordSagaDuration(order, "CANCELLED");
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

        recordSagaDuration(order, "COMPLETED");
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

        recordCompensation("SHIPMENT_FAILED");
        recordSagaDuration(order, "FAILED");
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

    /**
     * Records the {@code orbit.saga.duration} {@link Timer}, called from
     * every one of the 4 handlers above that transitions an order to a
     * terminal state ({@code COMPLETED}/{@code CANCELLED}/{@code FAILED}).
     * Duration is measured from {@link Order#getCreatedAt()} (the
     * {@code OrderCreated} event's timestamp) to now — the full wall-clock
     * lifetime of the Saga for this order, across every participant
     * service's async round-trip.
     *
     * <p>Tagged with {@code outcome}, a fixed 3-value discriminator
     * ({@code COMPLETED}/{@code CANCELLED}/{@code FAILED}) — bounded
     * cardinality, safe for Prometheus.
     *
     * <p>{@code publishPercentileHistogram()} makes Micrometer's Prometheus
     * registry export cumulative histogram buckets (the
     * {@code orbit_saga_duration_seconds_bucket} series) alongside the
     * usual {@code _count}/{@code _sum}, which is what lets the Grafana
     * Saga dashboard (Part E) compute p50/p95 via
     * {@code histogram_quantile(...)} — a plain {@link Timer} with no
     * histogram configured only exports {@code _count}/{@code _sum}/
     * {@code _max}, which is enough for an average but not a percentile.
     */
    private void recordSagaDuration(Order order, String outcome) {
        Duration duration = Duration.between(order.getCreatedAt(), Instant.now());
        Timer.builder(SAGA_DURATION_METRIC)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(duration);
    }

    /**
     * Increments the {@code orbit.saga.compensation} {@link Counter},
     * called only from the 3 handlers above that trigger a compensation
     * (stock release and/or payment refund).
     *
     * <p>Tagged with {@code trigger}, a fixed 3-value discriminator
     * ({@code STOCK_REJECTED}/{@code PAYMENT_DECLINED}/
     * {@code SHIPMENT_FAILED} — which {@code handleXxx} method fired) —
     * deliberately NOT the triggering event's free-text {@code reason}
     * field, which would create one Prometheus time series per distinct
     * reason string ever seen (unbounded cardinality, a well-known
     * footgun). See {@code docs/decisions.md} ("Phase 9 — Observability")
     * for the full tradeoff writeup.
     */
    private void recordCompensation(String trigger) {
        Counter.builder(SAGA_COMPENSATION_METRIC)
                .tag("trigger", trigger)
                .register(meterRegistry)
                .increment();
    }
}
