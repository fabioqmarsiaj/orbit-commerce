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
- [x] P0 Spring Boot 4 setup (Java 21) — virtual threads not yet enabled
- [x] P0 `Order` domain model as an event-sourced aggregate (states: CREATED,
      STOCK_RESERVED, PAYMENT_APPROVED, SHIPPED, COMPLETED, CANCELLED, FAILED) —
      `domain/Order.java`, sealed `OrderDomainEvent` hierarchy, exhaustive
      pattern-matching `apply()`, `create`/`rehydrate`/command methods implemented
- [x] P0 Event store (`order_events` table) + `outbox` table — `OrderEventStore`,
      `OrderEventMapper` (Jackson 3), `OutboxWriter`/`OutboxPublisher` (Avro JSON
      codec) implemented; both entities implement `Persistable<UUID>` since ids are
      assigned by application code, not the database
- [x] P0 REST API: `POST /orders`, `GET /orders/{id}` — implemented and verified live
      (curl/Invoke-RestMethod against a running instance)
- [x] P0 Publish `OrderCreated` via outbox + poller (@Scheduled) — implemented and
      verified: outbox row transitions PENDING -> PUBLISHED and the Avro message is
      observable on `order.events` via the console consumer
- [x] P0 Saga orchestration: send commands (ReserveStockCommand, ProcessPaymentCommand,
      CreateShipmentCommand) in reaction to received events — `OrderCommandService`
      + `SagaCommandFactory` implemented; verified `ReserveStockCommand` is published
      to `inventory.commands` right after order creation
- [x] P0 Listeners for StockReserved/StockRejected, PaymentApproved/PaymentDeclined,
      ShipmentCreated/ShipmentFailed — implemented using class-level
      `@KafkaListener` + method-level `@KafkaHandler` per topic (multi-type routing)
- [x] P0 Compensation logic (cancellation, refund, stock release) — implemented on
      both `Order` (raises OrderCancelled/OrderFailed) and `OrderCommandService`
      (sends ReleaseStockCommand/RefundPaymentCommand); not yet exercised end-to-end
      since inventory/payment/shipping-service don't exist until Phases 3-5
- [ ] P1 Idempotent event consumption (deduplication by eventId)
- [ ] P1 Correlation ID (orderId) propagated via Kafka headers
- [ ] P1 Integration tests with Testcontainers (Kafka + Postgres) — happy path and
      at least 1 compensation path

Manually verified end-to-end against the Phase 1 Docker Compose stack: `POST /orders`
creates the aggregate, persists to `order_events` and `outbox` in one transaction,
the outbox poller publishes the Avro-encoded `OrderCreated` to `order.events`, a
`ReserveStockCommand` is sent to `inventory.commands`, and `GET /orders/{id}` correctly
rehydrates and returns the aggregate. Two PostgreSQL/Hibernate pitfalls were found and
fixed along the way: (1) entities with application-assigned UUID ids need
`Persistable<UUID>` or Spring Data wrongly attempts `merge()`/UPDATE instead of
`persist()`/INSERT; (2) `@Lob String` maps to PostgreSQL's `oid` large-object type,
which fails outside an active transaction — plain `columnDefinition = "TEXT"` is the
correct mapping for JSON-sized text payloads. Also note Spring Boot 4 defaults to
**Jackson 3** (`tools.jackson.databind.ObjectMapper`), not the classic Jackson 2.

## Phase 3 — inventory-service
- [x] P0 Spring Boot 4 + Postgres setup (inventory_db) — `server.port=8083`
- [x] P0 Per-product stock model + initial seed endpoint — `POST /stock` (upsert),
      `GET /stock/{productId}`; `stock` table keyed by natural `productId`, plus a
      `stock_reservations` audit table keyed by (orderId, productId)
- [x] P0 `ReserveStockCommand` consumer → writes StockReserved/StockRejected to outbox
      — `InventoryCommandListener` + `InventoryCommandService` + `StockService`
- [x] P0 `ReleaseStockCommand` consumer (compensation) → StockReleased
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

Manually verified end-to-end against order-service + the Phase 1 Docker Compose
stack, both the happy path and the rejection path: `POST /orders` (with stock
seeded below the requested quantity) results in inventory-service publishing
`StockRejected`, order-service consuming it and transitioning the order to
`CANCELLED`, and — confirmed via `GET /stock/{productId}` — the stock level is
left completely unchanged (the in-transaction rollback described below works).
The happy path (sufficient stock) correctly decrements `quantity_available` /
increments `quantity_reserved` and results in order-service sending
`ProcessPaymentCommand` next.

Reservation is "all or nothing" per order: `StockService.tryReserveAll` reserves
each line item via a single atomic conditional `UPDATE ... WHERE quantity_available
>= :quantity` (see `StockRepository#tryReserve`), sorts items by `productId` first
to avoid deadlocking against other orders reserving the same products concurrently,
and — if any item can't be reserved — releases everything already reserved earlier
in the same call, all within the one `@Transactional` method, so the transaction as
a whole still commits (recording either `StockReserved` or `StockRejected` to the
outbox) rather than rolling back and losing that record.

Two more real bugs were found and fixed while testing this phase end-to-end
(neither specific to inventory-service — both affect any topic/outbox in the
project, so also relevant to Phases 4-5): (1) Kafka topics carrying more than one
Avro record type (`order.events`, `inventory.commands`, `inventory.events`, and
later `payment.*`/`shipping.*`) fail Schema Registry compatibility checks under the
default `TopicNameStrategy`, once a second, structurally different event type is
published to the same topic — fixed by switching producers to
`RecordNameStrategy`; (2) `OutboxPublisher.publishPending()` originally wrapped the
entire batch of pending rows in one `@Transactional` method — a failure partway
through the batch rolled back rows that had ALREADY been sent to Kafka
successfully, causing them to be re-published (duplicated) on the next poll; fixed
by publishing each row in its own transaction via a programmatic
`TransactionTemplate`. See `docs/decisions.md` for the full writeup of both.

### Post-Phase 3: extracted shared `outbox-support` module
Once inventory-service was the second service with a near-identical copy
of the Outbox pattern's plumbing (`OutboxEntity`, `OutboxStatus`,
`OutboxRepository`, `OutboxPublisher`), that duplication (~700 lines
across the two services) was extracted into a new library module,
`outbox-support` (added to `settings.gradle.kts`, no Spring Boot plugin —
same shape as `event-schemas`), before it could be copy-pasted a third
time into payment-service. `order-service` and `inventory-service` now
both `implementation(project(":outbox-support"))` and keep only a thin,
service-specific `OutboxWriter` (builds this service's Avro events) and
`OutboxPublisher extends AbstractOutboxPublisher` (just a
`toAvroRecord(eventType, payload)` switch + the `@Scheduled` method).
Required adding explicit `scanBasePackages`/`@EntityScan`/
`@EnableJpaRepositories` to both services' `@SpringBootApplication`
classes, since `com.fabioqmarsiaj.outbox` is a sibling package, not a
subpackage, of either service's own base package. Both services' test
suites (Testcontainers) and the full manual end-to-end walkthrough
(happy path + rejection path) were re-run and pass identically to
before the extraction. See `docs/decisions.md` ("Post-Phase 3 refactor —
extracting `outbox-support`") for the full writeup, including two Gradle
module-plumbing gotchas hit while creating the new module
(`java-library` vs `java` plugin for `api(...)` dependencies; needing to
explicitly add `jakarta.persistence-api` since it's not transitively
pulled in outside of `spring-boot-starter-data-jpa`).

### Post-Phase 3: routed Saga commands through the outbox too
The Phase 2 design deliberately sent Saga commands
(ReserveStockCommand/ProcessPaymentCommand/CreateShipmentCommand/
ReleaseStockCommand/RefundPaymentCommand) directly via `KafkaTemplate`,
bypassing the outbox — flagged at the time as a meaningful inconsistency
worth revisiting. Working through exactly what could go wrong surfaced a
real gap: `KafkaTemplate#send` returns a `CompletableFuture` that was
never awaited, so an asynchronous send failure (e.g. a transient broker
outage) after the transaction had already committed would be silently
swallowed — the order would advance in the database but the command
driving the Saga forward would simply never arrive, with no error and no
retry. Fixed by routing all 6 command-sending call sites in
`OrderCommandService` through the outbox instead (a new
`OutboxWriter#writeCommand` + 5 more cases in `OutboxPublisher`'s
`toAvroRecord` switch) — no schema change needed, since
`outbox-support`'s `OutboxEntity`/`OutboxRecorder`/
`AbstractOutboxPublisher` were already generic over `(aggregateId, topic,
SpecificRecord)` with no built-in assumption that rows have to be
"events" rather than "commands". `OrderCommandService`'s `KafkaTemplate`
dependency was removed entirely. Manually verified via
`POST /orders`: `order_db.outbox` shows `OrderCreated`,
`ReserveStockCommand`, and `ProcessPaymentCommand` all transitioning
`PENDING` → `PUBLISHED`, and inventory-service still correctly receives
and reacts to `ReserveStockCommand`. See `docs/decisions.md`
("Post-Phase 3 hardening — routing Saga commands through the outbox")
for the full writeup.

## Phase 4 — payment-service
- [x] P0 Spring Boot 4 + Postgres setup (payment_db) — `server.port=8084`
- [x] P0 `ProcessPaymentCommand` consumer → simulates approval/decline (simple rule,
      amountCents > `payment.approval.limit-cents` = decline) → PaymentApproved/
      PaymentDeclined via outbox — `PaymentCommandListener` + `PaymentCommandService`
- [x] P0 `RefundPaymentCommand` consumer (compensation) → PaymentRefunded
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

Depends on `outbox-support` (`implementation(project(":outbox-support"))`)
from the start, per the Phase 3 recommendation — thin `OutboxWriter` +
`OutboxPublisher extends AbstractOutboxPublisher`, `@EntityScan`/
`@EnableJpaRepositories`/`scanBasePackages` widened on
`PaymentServiceApplication`, and `RecordNameStrategy` set from the start.

`payments` table (`PaymentEntity`, keyed by `orderId`) is load-bearing,
not just audit — `RefundPaymentCommand` doesn't carry the `paymentId`
`PaymentApproved` generated, so it must be looked up. Both APPROVED and
DECLINED outcomes are persisted. A refund for a payment that isn't
currently APPROVED (missing, or already REFUNDED) is logged and skipped
rather than publishing a possibly-incorrect `PaymentRefunded`. See
`docs/decisions.md` ("Phase 4 — payment-service") for the full design
writeup.

Manually verified: happy path (`POST /orders` with amount under the
limit) results in `PaymentApproved` and order-service sending
`CreateShipmentCommand` next. The decline + compensation path (amount
over the limit) surfaced a real bug — see below.

### Post-Phase 4 fix: `@KafkaListener`s were missing a handler for their 3rd event type
Testing the decline path for the first time (payment declined → order
cancelled → stock released) was also the first time `StockReleased` was
ever actually published in this project — and it wedged order-service's
Kafka consumer: `InventoryEventListener`'s `@KafkaListener` had
`@KafkaHandler` methods for `StockReserved`/`StockRejected` but not
`StockReleased`, and an unhandled type throws `KafkaException: No method
found` rather than being silently skipped (the class's own Javadoc,
written in Phase 2, incorrectly claimed the latter) — with no custom
error handler, the container retries the same offset forever, wedging
the whole partition. `PaymentEventListener` had the identical latent bug
for `PaymentRefunded`, not yet triggered live only because
shipping-service doesn't exist yet. Fixed by adding a minimal
(log-and-return) `@KafkaHandler` for both previously-missing event types,
and rewriting both classes' Javadoc to state the rule explicitly: every
publishable event type on a multi-type topic needs an explicit handler
on every listener of that topic, with no exceptions. See
`docs/decisions.md` ("Post-Phase 4 fix — every Kafka event type on a
topic needs its own @KafkaHandler") for the full incident writeup.

## Phase 5 — shipping-service
- [x] P0 Spring Boot 4 + Postgres setup (shipping_db) — `server.port=8085`
- [x] P0 `CreateShipmentCommand` consumer → simulates shipment creation (forced to fail via
      a sentinel `customerId`, `shipping.simulation.force-fail-customer-id`, to test
      compensation) → ShipmentCreated/ShipmentFailed via outbox — `ShippingCommandListener`
      + `ShippingCommandService`
- [ ] P1 Idempotent consumption
- [ ] P1 Integration tests (Testcontainers)

Depends on `outbox-support` from the start, same as Phase 4. `CreateShipmentCommand`
carries no business field (amount, items) to key a realistic pass/fail decision on
— unlike inventory-service's stock check or payment-service's amount threshold — so
a sentinel `customerId` is used instead, giving the same deterministic per-request
test control. `ShipmentEntity` (keyed by `orderId`) is audit/query only, not
load-bearing: unlike payment-service's `PaymentEntity`, there is no compensating
command that flows back into shipping-service (a shipment failure is terminal;
order-service compensates payment/inventory directly, never asks shipping-service
to undo anything). See `docs/decisions.md` ("Phase 5 — shipping-service") for the
full design writeup.

Manually verified — the full Saga, end-to-end, for the first time:
- **Happy path**: `POST /orders` → stock reserved → payment approved →
  `ShipmentCreated` published → order reaches `COMPLETED`.
- **Shipment failure → dual compensation**: `POST /orders` with the sentinel
  `customerId` → order proceeds through stock reservation and payment approval,
  then `ShipmentFailed` triggers BOTH `RefundPaymentCommand` and
  `ReleaseStockCommand` in the same transaction (via the outbox) → payment marked
  `REFUNDED`, stock released back → order reaches `FAILED`. Confirmed directly in
  `payment_db.payments` and `inventory_db.stock`.

## Phase 6 — End-to-end Saga
- [x] P0 Validate full happy path (order → stock → payment → shipping → completed)
      — verified in Phase 5 above
- [x] P0 Validate compensation: stock rejected → order cancelled — verified in
      Phase 3
- [x] P0 Validate compensation: payment declined → stock released → order cancelled
      — verified in Phase 4
- [x] P0 Validate compensation: shipment failure → refund + stock release → order
      failed — verified in Phase 5 above
- [ ] P1 Document sequence diagrams (Mermaid) for the 4 scenarios above

All four P0 verification scenarios are now done — each was validated manually,
end-to-end, against real running services as the corresponding participant service
was implemented, rather than deferred to a separate dedicated testing phase. What
remains for this phase is purely the P1 documentation task (Mermaid sequence
diagrams) — worth revisiting once Phase 7-8 are done and the full picture is
stable, or as part of Phase 10 polish.

## Phase 7 — query-service (CQRS + Kafka Streams)
- [x] P0 Spring Boot 4 + Postgres setup (query_db, read model) — `server.port=8086`
- [x] P0 Consumers for order.events/inventory.events/payment.events/shipping.events →
      materialize order timeline (read model) — `OrderEventListener`/
      `InventoryEventListener`/`PaymentEventListener`/`ShippingEventListener` +
      `TimelineRecorder`/`OrderSummaryProjector`
- [x] P0 REST API: `GET /orders/{id}/timeline`, `GET /orders?status=`
- [x] P1 Kafka Streams topology over `user-activity.events`: tumbling window (1 min) of
      ProductViewed grouped by productId, count via state store — `UserActivityStreamsConfig`
- [x] P1 Interactive query endpoint: `GET /analytics/top-products` — `TopProductsQueryService`
      + `AnalyticsController`
- [ ] P2 Integration tests (Testcontainers, incl. Kafka Streams TopologyTestDriver)

query-service is architecturally different from every prior service: a pure CQRS
read-model consumer, never a Saga participant. It never publishes and does NOT
depend on `outbox-support` (that module is producer-side only) — no
`[Producer]` section in `application.properties`, no `@EntityScan`/
`@EnableJpaRepositories` widening needed either, since it never reaches into the
sibling `com.fabioqmarsiaj.outbox` package.

Two separate read models: `TimelineEntryEntity` (append-only, one row per event
across all 4 `*.events` topics) and `OrderSummaryEntity` (one row per order,
projected EXCLUSIVELY from `order.events` to avoid a cross-topic race — so its
4-value `OrderSummaryStatus` can't represent the 3 intermediate transitions
`STOCK_RESERVED`/`PAYMENT_APPROVED`/`SHIPPED`, only `CREATED`/`COMPLETED`/
`CANCELLED`/`FAILED`; those 3 remain fully visible via the granular timeline
endpoint). `TimelineRecorder` serializes plain Jackson 3 JSON (a
`Map<String,Object>` of just the fields worth showing), not Avro's own JSON
codec like every outbox writer — the payload here is only ever read back as a
generic map for the API response, never reconstructed into a typed Avro object.
See `docs/decisions.md` ("Phase 7 — query-service (Part A: read model)") for the
full design writeup.

Manually verified end-to-end with all 5 services running: both the happy path
and a failure/compensation path correctly show the full cross-topic event
sequence via `GET /orders/{id}/timeline`, and `GET /orders?status=` correctly
reflects each order's terminal status.

### Part B: Kafka Streams analytics
`spring-boot-starter-kafka` (already a dependency everywhere) already transitively
brings `@EnableKafkaStreams`/`StreamsBuilderFactoryBean`, and Spring Boot's own
`spring-boot-kafka` autoconfiguration auto-builds the required
`KafkaStreamsConfiguration` bean from `spring.kafka.streams.*` properties once that
bean is registered — so the only new dependency needed was
`io.confluent:kafka-streams-avro-serde:7.6.13`; no manual `KafkaStreamsConfiguration`
bean was written. `user-activity.events` carries 3 Avro types sharing one topic
(same multi-type-topic shape as everywhere else), so the topology reads
`GenericRecord` (via `GenericAvroSerde`) and filters down to `ProductViewed` by
checking `record.getSchema().getName()` — the Kafka-Streams-side equivalent of the
`eventType` discriminator pattern used throughout this project.
`GET /analytics/top-products?limit=` queries only the current (most recent) 1-minute
window via `StreamsBuilderFactoryBean` + `ReadOnlyWindowStore` — returns an empty
list until ingestion-service (Phase 8) produces real `ProductViewed` traffic, which
is expected, not a bug.

A real bug was found and fixed via query-service's own (unchanged) test suite:
Kafka Streams treats a missing SOURCE topic very differently from a plain
`@KafkaListener` (which just waits/retries) — it throws `MissingSourceTopicException`
and leaves the `StreamThread` PERMANENTLY in the `ERROR` state, which doesn't fail
`contextLoads()` (nothing asserts on Streams health) so the build reported success
while the Streams thread was silently broken every test run, because the ephemeral
Testcontainers test broker starts with no topics pre-created (unlike the real
`docker-compose` broker). Fixed by declaring `user-activity.events` as a
Spring-managed `NewTopic` bean, which `KafkaAdmin` idempotently ensures exists
before the Streams thread starts (a no-op against the real broker). See
`docs/decisions.md` ("Phase 7 — query-service (Part B: Kafka Streams analytics)")
for the full writeup — this pattern (`NewTopic` bean per Streams source topic)
should be reused for any future service adding its own Kafka Streams topology.

## Phase 8 — ingestion-service (Go)
- [x] P0 Go project setup (kafka-go or confluent-kafka-go) — `segmentio/kafka-go`
      (pure Go, no cgo) + `hamba/avro/v2` (not `goavro`, deprecated by its own
      maintainer in favor of hamba/avro)
- [x] P0 Activity event generator (ProductViewed, AddedToCart, SearchPerformed) using a
      worker pool/goroutines to simulate high volume — `internal/activity.Generator`
      (session-correlated funnel: search -> view -> maybe cart) +
      `internal/worker.Pool` (goroutine pool, shared aggregate rate limiter)
- [x] P0 Avro serialization compatible with Schema Registry, publishing to
      `user-activity.events` — `internal/schema.Registrar` (RecordNameStrategy,
      same as every Java producer) + `internal/producer.EncodeConfluentWire`
      (hand-rolled Confluent wire format: magic byte + schema ID + Avro binary)
- [x] P1 Simple HTTP endpoint (e.g. `/health`, `/stats`) using net/http or chi —
      `internal/httpapi` (net/http alone, `http.ServeMux` method routing)
- [x] P1 Configuration via env vars (event rate, worker count) — already scaffolded
      in Phase 0's `internal/config`, extended with `HTTP_PORT`
- [x] P2 Go unit tests (table-driven tests) — `internal/activity/generator_test.go`
      (funnel invariants: at least 1 view/session, shared sessionId, AddedToCart
      always references an already-viewed product, weighted type distribution) +
      `internal/producer/encode_test.go` (Confluent wire-format round-trip)

ingestion-service is architecturally different from every Java service: it never
participates in the Saga and has no outbox/retry mechanism of its own — a failed
publish is logged and counted (`GET /stats`'s `publishErrors`), not retried. This
is a deliberate, documented asymmetry with the rest of the project (simulated,
best-effort traffic has no domain state to keep consistent), not an oversight.

Manually verified end-to-end: `go run ./cmd/ingestion-service` against the Phase 1
Docker Compose stack registered all 3 `user-activity.events` schemas with Schema
Registry, and published messages whose count exactly matched Kafka's own reported
partition offsets. Running `query-service` (Phase 7 Part B) against this real
traffic — the first time that Kafka Streams topology ever processed real data —
immediately surfaced a real `ClassCastException` bug in `UserActivityStreamsConfig`
(`GenericRecord#get` returns Avro's `Utf8` wrapper for string fields, not
`java.lang.String`; fixed by using `.toString()` instead of an unchecked cast).
After the fix, `GET /analytics/top-products` returned real, non-empty results for
the first time. See `docs/decisions.md` ("Phase 8 — ingestion-service") for the
full design writeup, including the library choices (hamba/avro/v2 vs goavro,
kafka-go vs confluent-kafka-go) and the bug investigation.

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
