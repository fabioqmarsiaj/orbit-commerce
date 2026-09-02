# Technical Decisions Log

This file captures non-obvious technical decisions, pitfalls, and their
resolutions made while building orbit-commerce. It exists so that context
isn't lost between sessions (compacted or brand new) — the code itself has
Javadoc explaining "why" at the point of use, but this file is the place to
look for the broader reasoning and things worth remembering across phases.

Organized roughly in the order they were made. Newest entries at the bottom
of each phase section.

---

## Phase 0 — Scaffolding

### Gradle plugin id for Avro is `com.github.davidmc24.gradle.plugin.avro`
Not `com.github.davidmc24.gradle.avro` (doesn't exist) or any other
variation — this tripped up the initial search. Version `1.9.1` is the
latest as of this writing, built against Avro 1.11.3. Verified via the
Gradle Plugin Portal maven-metadata.xml before using it.

### Package base is `com.fabioqmarsiaj`, not `dev.orbitcommerce`
Early scaffolding used `dev.orbitcommerce.<service>` as the package base;
this was corrected to `com.fabioqmarsiaj.<service>` per explicit
instruction. All 5 Spring Boot modules and the Go module
(`github.com/fabioqmarsiaj/orbit-commerce/ingestion-service`) follow this.

### Monorepo structure: one root `build.gradle.kts` + wrapper, not one per module
Spring Initializr generates a full standalone Gradle project (its own
`settings.gradle.kts`, `gradlew`, `gradle/wrapper/`) per module when you
download a starter zip. After extracting each of the 5 service zips into
the monorepo, those per-module Gradle scaffolding files were deleted and a
single root `settings.gradle.kts` (with `include(...)` for every module)
and root `build.gradle.kts` (with shared plugin versions via `apply
false` + `subprojects {}` block) were created instead. The root Gradle
wrapper was regenerated via `gradle wrapper --gradle-version 9.5.1`.

### `.gitattributes` needed for shell scripts on Windows
Without `* text=auto eol=lf` + explicit `*.sh text eol=lf`, Git on Windows
converts `infra/kafka/init-topics.sh` and `infra/postgres/init-databases.sh`
to CRLF line endings, which breaks `#!/bin/bash` execution inside the Linux
containers that run them. Fixed via `.gitattributes` + `git add --renormalize .`.

---

## Phase 1 — Docker Compose infrastructure

### Kafka image: `apache/kafka:4.3.1`, KRaft combined mode
Official Apache image, no Zookeeper. Single node acts as both broker and
controller (`KAFKA_PROCESS_ROLES: broker,controller`). Two listeners are
configured: `PLAINTEXT` (internal, `broker:19092`, used by other
containers and by `kafka-init`) and `PLAINTEXT_HOST` (external,
`localhost:9092`, used by services running on the host machine like
order-service via `bootRun`).

### Kafka UI: `kafbat/kafka-ui`, NOT `provectuslabs/kafka-ui`
The originally-suggested `provectuslabs/kafka-ui` image has not been
updated in 2+ years (project effectively abandoned/superseded). `kafbat/kafka-ui`
is the actively maintained community fork (images published within days at
the time of writing). Verified via Docker Hub tag timestamps before switching.

### Topic creation via a one-shot `kafka-init` container, not `KAFKA_AUTO_CREATE_TOPICS_ENABLE`
Auto-create is explicitly disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`)
so that topic configuration (partition count, replication factor) is
deliberate and versioned in `infra/kafka/init-topics.sh`, rather than
implicitly created with default settings the first time any producer/consumer
touches a topic. The init container waits for the broker healthcheck, creates
all 8 topics idempotently (`--if-not-exists`), then exits (exit code 0 is
expected and correct — it's not meant to keep running).

### Postgres: single instance, multiple databases (not one container per service)
`infra/postgres/init-databases.sh` runs once (via
`docker-entrypoint-initdb.d/`) and creates `order_db`, `payment_db`,
`inventory_db`, `shipping_db`, `query_db` — one database per service,
sharing one Postgres container. Keeps `docker-compose.yml` lighter for
local development while still giving each service its own schema/database
(logical isolation), which is the property that actually matters for the
microservices "database per service" principle in a learning context.

---

## Phase 2 — order-service (Saga orchestrator, Event Sourcing, Outbox)

### Spring Boot 4 defaults to Jackson 3, not Jackson 2
This is the single biggest gotcha of Phase 2. Spring Boot 4's
`spring-boot-starter-webmvc` pulls in `spring-boot-starter-jackson`, which
wires up **`tools.jackson.databind.ObjectMapper`** (Jackson 3, new Maven
group `tools.jackson.*`) as the auto-configured bean — NOT the classic
`com.fasterxml.jackson.databind.ObjectMapper` (Jackson 2) that essentially
all existing tutorials/StackOverflow answers assume. Jackson 2 support in
Spring Boot 4 "ships in a deprecated form" per the official 4.0 release
notes.

Practical consequences:
- `OrderEventMapper` imports `tools.jackson.databind.ObjectMapper`.
- Jackson 3's `JacksonException` (thrown by `writeValueAsString`/`readValue`)
  is an **unchecked** `RuntimeException`, unlike Jackson 2's checked
  `JsonProcessingException` — no `try/catch` is needed purely for
  compilation, though we still want to catch/wrap it in places where a
  clearer error message helps (see `OrderEventMapper` for the current
  approach: mostly let it propagate, since our own records should never
  realistically fail to (de)serialize).

Symptom when this goes wrong: `UnsatisfiedDependencyException: ... No
qualifying bean of type 'com.fasterxml.jackson.databind.ObjectMapper'
available` at startup, if you import the Jackson 2 type by habit.

### Avro `SpecificRecord` objects must NOT be serialized with Jackson
Avro-generated classes (e.g. `com.fabioqmarsiaj.events.order.OrderCreated`)
expose a bean-style `getSchema()` getter returning `org.apache.avro.Schema`
— a complex object that generic Jackson bean introspection cannot cleanly
handle. Using a plain `ObjectMapper.writeValueAsString(avroRecord)` either
throws or produces garbage output.

The correct approach — used in `OutboxWriter`/`OutboxPublisher` — is
Avro's own schema-aware JSON codec:
- Encode: `EncoderFactory.get().jsonEncoder(schema, outputStream)` +
  `SpecificDatumWriter<T>`.
- Decode: `DecoderFactory.get().jsonDecoder(schema, jsonString)` +
  `SpecificDatumReader<T>`.

This is orthogonal to (and NOT used for) `OrderEventMapper`, which
serializes plain Java `record`s (the internal domain events) with regular
Jackson — those have no `getSchema()` and are not Avro types at all. Two
different serialization concerns living in two different classes on
purpose; don't conflate them.

### JPA entities with application-assigned UUID ids need `Persistable<UUID>`
Both `OrderEventEntity` and `OutboxEntity` generate their own `id` via
`UUID.randomUUID()` in application code (in `OrderEventStore.append` and
`OutboxWriter.writeAll` respectively) *before* the entity is even
constructed — this is necessary because the event/outbox row's id needs to
be known immediately, not assigned later by the database.

Spring Data JPA's default heuristic for "is this a new entity (INSERT via
`persist()`) or an existing one (UPDATE via `merge()`)" is "is the `@Id`
field null?". Since our ids are never null (assigned in the constructor),
Spring Data always guesses "existing" and calls `merge()` — which issues
an UPDATE that matches zero rows (the row doesn't exist yet), and Hibernate
raises `org.springframework.orm.ObjectOptimisticLockingFailureException` /
`org.hibernate.StaleObjectStateException: Row was already updated or
deleted by another transaction`.

Fix: both entities implement `org.springframework.data.domain.Persistable<UUID>`
and override `isNew()`:
- `OrderEventEntity.isNew()` always returns `true` — the `order_events`
  table is genuinely append-only, so every `save()` call really is a new
  row, forever.
- `OutboxEntity.isNew()` returns a `@Transient boolean isNew` field that is
  set to `true` only in the "brand new row" constructor. When
  `OutboxPublisher` later reloads a row from the repository and calls
  `save()` again after `markPublished()`, that instance went through JPA's
  no-arg constructor + field reflection (bypassing the app constructor
  entirely), so the transient flag naturally stays `false` — correctly
  telling Spring Data to `merge()`/UPDATE this time.

This is a common enough pitfall with UUID-primary-key + app-assigned-id
entities that it's worth remembering for inventory/payment/shipping-service
too, if they end up with a similar shape (e.g. their own outbox tables).

### `@Lob` on a `String` column breaks outside an active transaction on PostgreSQL
Both entities originally had `@Lob @Column(name = "payload", ...)` on their
JSON payload `String` field. On PostgreSQL, Hibernate maps `@Lob String` to
the `oid` large-object type by default, which requires the read to happen
inside an active database transaction — `GET /orders/{id}`
(`OrderEventStore.load`, called from a non-`@Transactional` controller
method) failed with:
```
org.postgresql.util.PSQLException: Large Objects may not be used in auto-commit mode.
```//`org.hibernate.orm.jpa.JpaSystemException: Unable to access lob stream`

Fix: removed `@Lob` entirely and used
`@Column(name = "payload", nullable = false, columnDefinition = "TEXT")`
instead. PostgreSQL's native `TEXT` type has no such restriction and is the
correct mapping for JSON-sized text payloads (this project's payloads are a
few hundred bytes to a few KB — nowhere near needing true LOB semantics).

Because `spring.jpa.hibernate.ddl-auto=update` doesn't retroactively change
an existing column's type, switching this required manually dropping
(`DROP TABLE order_events, outbox CASCADE;`) and letting Hibernate recreate
the tables on next startup, during local iteration.

### Saga commands (ReserveStockCommand etc.) do NOT go through the outbox — deliberately, for now
`OrderCommandService` sends Saga commands (`ReserveStockCommand`,
`ProcessPaymentCommand`, `CreateShipmentCommand`, `ReleaseStockCommand`,
`RefundPaymentCommand`) via a direct, synchronous `kafkaTemplate.send(...)`
call, immediately after the `@Transactional` method that persisted the
triggering domain event(s) returns — NOT via the outbox pattern.

This is an intentional simplification, not an oversight, but it IS a real
inconsistency worth remembering: if the process crashes between the DB
transaction committing and the `kafkaTemplate.send()` call completing, the
Saga command is lost — the order's state has already moved forward (e.g.
to `CREATED`) but no `ReserveStockCommand` ever reaches inventory-service,
and nothing will retry it. Contrast with `order.events` publishing, which
goes through the full outbox + poller pattern and is crash-safe
(at-least-once).

Possible future fix (not implemented): route Saga commands through the
outbox too, using the `topic` column `OutboxEntity` already has (it's not
hardcoded to `order.events` at the schema level, only `OutboxWriter`
currently always passes `KafkaTopics.ORDER_EVENTS`).

### Outbox publish is synchronous (`.join()`), by design, for at-least-once delivery
`OutboxPublisher.publishPending()` blocks on `kafkaTemplate.send(...).join()`
for each pending row before marking it `PUBLISHED` and saving. If the send
fails, the exception propagates, the `@Transactional` method rolls back,
and the row stays `PENDING` for the next poll (every 2s by default,
`outbox.publisher.fixed-delay-ms`) to retry. This trades throughput
(rows are published one at a time, not pipelined) for the correctness
guarantee that a row is never marked `PUBLISHED` unless Kafka actually
acknowledged it — irrelevant at this project's volume, but worth
understanding as a deliberate choice, not an oversight.

### Kafka listeners for multi-type topics: class-level `@KafkaListener` + method-level `@KafkaHandler`
`inventory.events`, `payment.events`, and `shipping.events` each carry more
than one Avro message type (e.g. `inventory.events` carries
`StockReserved`, `StockRejected`, AND `StockReleased`, even though
order-service only cares about the first two). Spring Kafka's idiomatic
way to route different deserialized types arriving on the same topic to
different handler methods is to put `@KafkaListener(topics = ...)` on the
class and `@KafkaHandler` on each method that should handle one specific
type — the container dispatches based on the runtime type of the
deserialized payload. A message type with no matching `@KafkaHandler`
(like `StockReleased`) is simply not delivered anywhere; no error.

### Logging: Lombok `@Slf4j`, `INFO` level, three checkpoints per Saga step
Added after the fact (not part of the original Phase 2 scaffold) to make
manual testing observable. `@Slf4j` was chosen over hand-written
`LoggerFactory.getLogger(...)` since Lombok was already a dependency but
unused. Three log points per Saga-advancing method in
`OrderCommandService`/`OutboxPublisher`:
1. `OrderCommandService.persistAndPullEvents(...)` — logs after both
   `eventStore.append(...)` and `outboxWriter.writeAll(...)` succeed:
   `"Order {}: persisted {} event(s) to order_events and outbox"`.
2. Immediately after each `kafkaTemplate.send(...)` that dispatches a Saga
   command: `"Order {}: sent <CommandName> to topic {}"`.
3. `OutboxPublisher.publishPending()` — logs after `row.markPublished(...)`
   + `repository.save(row)`: `"Outbox: published {} for order {} to topic {}"`.

These three checkpoints let you trace, from log timestamps alone, the full
async gap between "domain event persisted" (synchronous, inside the HTTP
request) and "actually visible on Kafka" (asynchronous, up to
`outbox.publisher.fixed-delay-ms` later) — this was the whole point of the
manual walkthrough that led to writing this decisions log in the first
place.

### Local port: order-service runs on 8082, not 8080
8080 is already used by Kafka UI in `docker-compose.yml`. `server.port=8082`
is set in `order-service/src/main/resources/application.properties`.
Future services (inventory/payment/shipping/query) will need their own
distinct ports if run simultaneously on the host outside Docker — worth
picking a convention (e.g. 8082, 8083, 8084, 8085...) before Phase 3.

### Manual end-to-end verification performed for Phase 2
Not automated (Testcontainers integration tests deliberately deferred —
see below), but manually verified against the Phase 1 Docker Compose
stack + `bootRun`:
- `POST /orders` → `201 Created`, aggregate persisted to `order_events`
  + `outbox` in one transaction.
- `GET /orders/{id}` → `200 OK`, correctly rehydrates full aggregate state
  from `order_events`.
- Outbox row transitions `PENDING` → `PUBLISHED` within the poll interval.
- `OrderCreated` Avro message observed on `order.events` via
  `kafka-console-consumer` (raw bytes, readable strings for `customerId`/
  `productId` confirmed correct).
- `ReserveStockCommand` Avro message observed on `inventory.commands`,
  correctly keyed by `orderId`.
- All three log checkpoints (see above) observed in the right order with
  the expected ~1-2s gap between persistence and outbox publish.

### Testcontainers integration tests deliberately deferred
Decision made explicitly: skip writing Testcontainers-based integration
tests for now (for order-service and likely for inventory/payment/shipping-
service too), revisit at the very end of the project if there's time/interest.
Manual end-to-end verification via `bootRun` + curl/Postman + Kafka UI +
`psql` is the verification method being used instead across phases. If this
changes, update `TASKS.md` Phase 2 Block F and this note.

---

## Cross-cutting notes for Phase 3+ (inventory-service and beyond)

Things established in Phase 2 that will likely recur:

- Reuse the `Persistable<UUID>` pattern for any append-only or
  app-assigned-id entity in inventory/payment/shipping-service.
- Reuse `columnDefinition = "TEXT"` instead of `@Lob` for any JSON string
  payload column on PostgreSQL.
- Remember Jackson 3 (`tools.jackson.databind.ObjectMapper`) if any service
  needs to hand-serialize domain events to JSON like `OrderEventMapper` does.
- Remember the Avro JSON codec (`EncoderFactory`/`DecoderFactory` +
  `SpecificDatumWriter`/`SpecificDatumReader`) if any service implements
  its own outbox (inventory/payment/shipping-service all publish events —
  `StockReserved`, `PaymentApproved`, `ShipmentCreated`, etc. — so they
  likely need the same outbox infrastructure order-service has).
- Decide up front whether inventory/payment/shipping-service's own Saga
  replies (their `*.events` topics) should go through an outbox from the
  start, given the known gap in order-service's Saga *commands* (see above).
- Pick a `server.port` for each new service before running multiple
  services simultaneously outside Docker (order-service = 8082).
