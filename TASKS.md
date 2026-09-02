# orbit-commerce — Task Breakdown

Priority legend: P0 (critical/blocking) · P1 (essential) · P2 (nice-to-have)

## Phase 0 — Repository scaffolding
- [x] P0 Create monorepo structure (Gradle multi-module + separate Go module)
- [x] P0 `settings.gradle.kts` with modules: order-service, payment-service, inventory-service,
      shipping-service, query-service, event-schemas
- [x] P0 `event-schemas` module: Avro schemas (.avsc) for order, inventory, payment, shipping,
      and user-activity events/commands
- [x] P0 Configure the Avro Gradle plugin for Java class generation
- [x] P1 `go.mod` for ingestion-service, Go folder structure (cmd/, internal/)
- [x] P1 `.gitignore` (Java + Go + Docker) — `.editorconfig` pending
- [x] P2 Initial README with architecture overview (placeholder)

## Phase 1 — Base infrastructure (Docker Compose)
- [x] P0 `docker-compose.yml`: Kafka in KRaft mode (no Zookeeper) — `apache/kafka:4.3.1`,
      combined broker+controller node
- [x] P0 Confluent Schema Registry — `confluentinc/cp-schema-registry:7.6.13`
- [x] P0 Single Postgres instance + init script creating DBs: order_db, payment_db,
      inventory_db, shipping_db, query_db — `postgres:16-alpine`
- [x] P1 Kafka UI for topic/schema inspection — `kafbat/kafka-ui:latest` (actively maintained
      community fork; the original `provectuslabs/kafka-ui` has been unmaintained for 2+ years)
- [x] P1 Kafka topic creation via init script (order.events, inventory.commands,
      inventory.events, payment.commands, payment.events, shipping.commands, shipping.events,
      user-activity.events) — one-shot `kafka-init` container running `infra/kafka/init-topics.sh`
- [ ] P2 Prometheus + Grafana + kafka-exporter (deferred to Phase 9)
- [x] P1 PowerShell scripts (`scripts/env-up.ps1`, `env-down.ps1`, `env-logs.ps1`) to bring the
      local environment up/down

Validated end-to-end: broker healthy, all 8 topics created, 5 Postgres databases created,
Schema Registry and Kafka UI reachable over HTTP, and a real produce/consume round-trip
through `order.events` succeeded.

## Phase 2 — order-service (Saga core)
- [x] P0 Spring Boot 4 setup (Java 21) — scaffolded; virtual threads not yet enabled
- [~] P0 `Order` domain model as an event-sourced aggregate (states: CREATED,
      STOCK_RESERVED, PAYMENT_APPROVED, SHIPPED, COMPLETED, CANCELLED, FAILED) —
      class/method skeleton in place (`domain/Order.java`, sealed `OrderDomainEvent`
      hierarchy), state-machine logic left as TODOs to implement
- [~] P0 Event store (`order_events` table) + `outbox` table — JPA entities,
      repositories and `OrderEventStore`/`OutboxWriter` skeletons in place, core
      logic left as TODOs
- [~] P0 REST API: `POST /orders`, `GET /orders/{id}` — controller + DTOs
      scaffolded, wiring left as TODOs
- [~] P0 Publish `OrderCreated` via outbox + poller (@Scheduled) — `OutboxPublisher`
      skeleton in place (`@Scheduled` wired, `@EnableScheduling` added), Avro
      encode/decode + Kafka send left as TODOs
- [~] P0 Saga orchestration: send commands (ReserveStockCommand, ProcessPaymentCommand,
      CreateShipmentCommand) in reaction to received events — `OrderCommandService`
      and `SagaCommandFactory` skeletons in place, orchestration logic left as TODOs
- [~] P0 Listeners for StockReserved/StockRejected, PaymentApproved/PaymentDeclined,
      ShipmentCreated/ShipmentFailed — listener classes scaffolded for all three
      topics, dispatch logic left as TODOs
- [~] P0 Compensation logic (cancellation, refund, stock release) — command methods
      stubbed on `Order` and `OrderCommandService`, left as TODOs
- [ ] P1 Idempotent event consumption (deduplication by eventId)
- [ ] P1 Correlation ID (orderId) propagated via Kafka headers
- [ ] P1 Integration tests with Testcontainers (Kafka + Postgres) — happy path and
      at least 1 compensation path

Legend: `[~]` = scaffolded (package/class/method structure + docs + TODOs in place,
compiles successfully) but business logic not yet implemented — being worked on
collaboratively as a learning exercise before marking fully complete.

## Phase 3 — inventory-service
- [ ] P0 Spring Boot 4 + Postgres setup (inventory_db)
- [ ] P0 Per-product stock model + initial seed endpoint
- [ ] P0 `ReserveStockCommand` consumer → writes StockReserved/StockRejected to outbox
- [ ] P0 `ReleaseStockCommand` consumer (compensation) → StockReleased
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

## Phase 4 — payment-service
- [ ] P0 Spring Boot 4 + Postgres setup (payment_db)
- [ ] P0 `ProcessPaymentCommand` consumer → simulates approval/decline (simple rule,
      e.g. amount > limit = decline) → PaymentApproved/PaymentDeclined via outbox
- [ ] P0 `RefundPaymentCommand` consumer (compensation) → PaymentRefunded
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

## Phase 5 — shipping-service
- [ ] P0 Spring Boot 4 + Postgres setup (shipping_db)
- [ ] P0 `CreateShipmentCommand` consumer → simulates shipment creation (can be forced to
      fail to test compensation) → ShipmentCreated/ShipmentFailed via outbox
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

## Phase 6 — End-to-end Saga
- [ ] P0 Validate full happy path (order → stock → payment → shipping → completed)
- [ ] P0 Validate compensation: stock rejected → order cancelled
- [ ] P0 Validate compensation: payment declined → stock released → order cancelled
- [ ] P0 Validate compensation: shipment failure → refund + stock release → order failed
- [ ] P1 Document sequence diagrams (Mermaid) for the 4 scenarios above

## Phase 7 — query-service (CQRS + Kafka Streams)
- [ ] P0 Spring Boot 4 + Postgres setup (query_db, read model)
- [ ] P0 Consumers for order.events/inventory.events/payment.events/shipping.events →
      materialize order timeline (read model)
- [ ] P0 REST API: `GET /orders/{id}/timeline`, `GET /orders?status=`
- [ ] P1 Kafka Streams topology over `user-activity.events`: tumbling window (1 min) of
      ProductViewed grouped by productId, count via state store
- [ ] P1 Interactive query endpoint: `GET /analytics/top-products`
- [ ] P2 Integration tests (Testcontainers, incl. Kafka Streams TopologyTestDriver)

## Phase 8 — ingestion-service (Go)
- [ ] P0 Go project setup (kafka-go or confluent-kafka-go)
- [ ] P0 Activity event generator (ProductViewed, AddedToCart, SearchPerformed) using a
      worker pool/goroutines to simulate high volume
- [ ] P0 Avro serialization compatible with Schema Registry, publishing to
      `user-activity.events`
- [ ] P1 Simple HTTP endpoint (e.g. `/health`, `/stats`) using net/http or chi
- [ ] P1 Configuration via env vars (event rate, worker count)
- [ ] P2 Go unit tests (table-driven tests)

## Phase 9 — Observability
- [ ] P1 Micrometer + Prometheus on all Spring services
- [ ] P1 Custom metrics: saga duration, compensation rate by failure type
- [ ] P1 kafka-exporter for consumer lag metrics
- [ ] P1 Grafana dashboards (throughput, saga latency, consumer lag)
- [ ] P2 Distributed tracing (OpenTelemetry + Jaeger/Tempo) — time permitting

## Phase 10 — Documentation and polish
- [ ] P0 Main README: overview, architecture, how to bring up the environment
- [ ] P0 Overall architecture diagram (Mermaid or image)
- [ ] P1 Saga sequence diagram (all scenarios)
- [ ] P1 Documented event catalog (table: topic, schema, producer, consumers)
- [ ] P2 Troubleshooting guide / architecture decisions (short ADRs)
- [ ] P2 GitHub Actions: build + tests in CI for all modules (Java and Go)
