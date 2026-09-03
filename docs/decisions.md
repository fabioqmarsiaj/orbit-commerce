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

## Phase 3 — inventory-service

### Multi-type Kafka topics break Schema Registry compatibility checks under the default subject strategy
This was discovered live, mid-testing: order-service's `OutboxPublisher`
threw `SerializationException` / `RestClientException` (HTTP 409) the
first time it ever tried to publish an `OrderCancelled` event —
`Schema being registered is incompatible with an earlier schema for
subject "order.events-value"`, with details `NAME_MISMATCH` (the record's
`name` changed) and `READER_FIELD_MISSING_DEFAULT_VALUE` (the new
`reason` field has no default).

Root cause: the Confluent Avro serializer's default subject naming
strategy is `TopicNameStrategy` — every message published to a given
Kafka topic is registered under ONE subject, named `<topic>-value`
(e.g. `order.events-value`), and the Schema Registry enforces
compatibility (`BACKWARD` by default, checked via `GET /config`) between
whatever gets registered there. That's the correct behavior for a topic
that carries a single, evolving schema — but `order.events` was designed
from Phase 2 onward to carry FOUR different, structurally unrelated
record types (`OrderCreated`, `OrderCompleted`, `OrderCancelled`,
`OrderFailed`) on the same topic. The bug had been latent since Phase 2:
nothing caught it earlier because until inventory-service actually
existed and could reply with `StockRejected`, order-service had only
ever published `OrderCreated` — the first "second, different" event type
published to `order.events` was what triggered it.

This is not specific to `order.events` — the exact same problem applies
to every other multi-type topic in the project: `inventory.commands`
(`ReserveStockCommand`/`ReleaseStockCommand`), `inventory.events`
(`StockReserved`/`StockRejected`/`StockReleased`), and — once
payment-service/shipping-service exist — `payment.commands`,
`payment.events`, `shipping.commands`, `shipping.events` too.

**Fix:** switch the Avro serializer's subject naming strategy to
`RecordNameStrategy`, which uses the Avro record's fully-qualified name
(e.g. `com.fabioqmarsiaj.events.order.OrderCreated`) as the subject
instead of the topic name — so every event type gets its own,
independent subject and compatibility history, and different event types
sharing a topic never get compared against each other:
```properties
spring.kafka.producer.properties.value.subject.name.strategy=io.confluent.kafka.serializers.subject.RecordNameStrategy
```
Only needs to be set on the **producer** side. Consumers never need it —
`KafkaAvroDeserializer` resolves the writer schema directly by the
numeric schema ID embedded in the Confluent wire format (magic byte + 4
bytes), not by re-deriving a subject name. Applied to both
`order-service` and `inventory-service`'s `application.properties`; will
need to be applied to `payment-service`/`shipping-service` too once they
exist.

The old `order.events-value` subject (registered under the previous
`TopicNameStrategy` behavior) was deliberately left in the Schema
Registry rather than deleted — it's orphaned/unused going forward but
harmless, and deleting subjects from a registry is a one-way,
slightly-risky operation not worth doing for a local dev environment.

### Outbox publisher: one transaction per row, not one per batch
Found and fixed alongside the bug above, while investigating the same
stack trace. `OutboxPublisher.publishPending()` originally wrapped the
ENTIRE loop over all pending rows in a single `@Transactional` method:
```java
@Scheduled(...)
@Transactional
public void publishPending() {
    for (OutboxEntity row : pending) {
        kafkaTemplate.send(...).join();   // Kafka already has it
        row.markPublished(...);
        repository.save(row);             // not committed yet (same tx)
    }
}
```
If any row partway through the batch throws (exactly what happened with
the `OrderCancelled` schema registration failure), Spring rolls back the
WHOLE transaction — including the `markPublished`/`save` for every row
earlier in that same loop iteration that had already been sent to Kafka
successfully. Kafka sends can't be un-sent, so those earlier rows revert
to `PENDING` in the database and get needlessly re-published (a
duplicate) on the very next poll. The loop also aborts entirely on the
first failure, so rows later in the same batch aren't even attempted
until the next poll cycle.

**Fix:** publish each row in its own, independent transaction, using a
programmatically-created `TransactionTemplate` (injecting
`PlatformTransactionManager` in the constructor) rather than the
declarative `@Transactional` annotation on the whole method:
```java
public void publishPending() {                 // no @Transactional here
    for (OutboxEntity row : pending) {
        try {
            transactionTemplate.executeWithoutResult(status -> publishRow(row));
        } catch (Exception e) {
            log.error("... will retry next poll", e);
            // deliberately not rethrown: keep trying the rest of the batch
        }
    }
}
```
A programmatic `TransactionTemplate` was necessary here (rather than just
extracting a private `@Transactional` method) because Spring's
`@Transactional` is implemented via a dynamic proxy wrapping the bean —
calling an annotated method from another method on the SAME instance
(`this.publishRow(row)`) bypasses the proxy entirely ("self-invocation"),
so the annotation would silently do nothing. `TransactionTemplate` starts
a real transaction directly, with no proxy involved, sidestepping that
pitfall.

Applied identically to both `order-service` and `inventory-service`'s
`OutboxPublisher`.

**Known limitation, not fixed:** this still assumes a single instance of
the poller runs at a time. If a service is ever scaled to multiple
replicas, two instances could both read and attempt to publish the same
`PENDING` row within the same poll window — each individual publish is
still safe (at-least-once), but duplicates become more likely. The fix
for that would be reading pending rows with `SELECT ... FOR UPDATE SKIP
LOCKED` so concurrent pollers naturally partition the work instead of
racing over the same rows — not implemented, since this project only
ever runs one instance of each service locally.

### "All or nothing" multi-item stock reservation via atomic conditional UPDATE + in-transaction compensation
`ReserveStockCommand` carries a list of line items (an order can request
several different products at once), and the whole reservation attempt
needs to succeed or fail as a unit — reserving 2 out of 3 items and
silently dropping the third would corrupt the Saga's assumptions.

Two design choices worth remembering here:
1. **Atomic conditional UPDATE instead of optimistic locking.** Each
   item is reserved via a single UPDATE with a `WHERE` guard:
   `UPDATE stock SET quantity_available = quantity_available - :qty ...
   WHERE product_id = :id AND quantity_available >= :qty` (see
   `StockRepository#tryReserve`), checking the number of rows affected
   (`0` = not enough stock) rather than reading the row, checking it in
   Java, and writing it back (which would race against a concurrent
   reservation for the same product). This was chosen over `@Version`
   optimistic locking (used nowhere in this project so far) specifically
   because it needs no retry loop: the database itself resolves the race
   as part of the single UPDATE statement.
2. **Items are reserved in a fixed order (sorted by `productId`) before
   attempting anything**, in `StockService.tryReserveAll`. Since Saga
   commands are Kafka-partitioned by `orderId` (not `productId`), two
   different orders that both touch the same two products could
   otherwise be processed concurrently by two different consumer
   threads, each attempting their UPDATEs in whatever order their own
   line items happen to be listed — a classic setup for a database
   deadlock (thread A holds product X's row lock and wants product Y's;
   thread B holds Y's and wants X's). Sorting first means every caller
   always acquires row locks in the same global order.

If any item in the sequence can't be reserved (returns `0` rows
updated), every item already reserved earlier IN THAT SAME CALL is
released again (`StockRepository#release`) — all still inside the one
`@Transactional` `InventoryCommandService.handleReserveStock` method,
rather than letting an exception propagate and trigger Spring's
transaction rollback. This is deliberate: the method needs to still
COMMIT either way, because it must record either `StockReserved` or
`StockRejected` to the outbox in the same transaction as the reservation
attempt's outcome — a rollback would silently discard that outbox row
too, defeating the whole point of the Outbox pattern (the write and the
"intent to publish" must succeed or fail together, and here "succeed"
includes the rejection case).

### `Persistable<String>` — the same pitfall, with a natural key instead of a UUID
`StockEntity` is keyed by `productId` (a natural/business key like
`"sku-123"`, supplied by the seed endpoint caller), not a randomly
generated UUID — but the exact same Spring Data JPA pitfall documented
for `order-service` in Phase 2 still applies: `productId` is never null
the moment the entity is constructed, so Spring Data's default
"is `@Id` null?" heuristic for `persist()` vs `merge()` still guesses
wrong. `StockEntity implements Persistable<String>`, using the identical
"`@Transient boolean isNew` flag set only in the brand-new-row
constructor" technique as `order-service`'s `OutboxEntity` (see Phase 2
section above) — confirming this pattern generalizes to any
non-null-by-construction `@Id`, whatever its type or origin, not just
app-generated UUIDs specifically.

---

## Post-Phase 3 refactor — extracting `outbox-support`

By the end of Phase 3, `order-service` and `inventory-service` each had
their own, nearly byte-for-byte identical copies of `OutboxEntity`,
`OutboxStatus`, `OutboxRepository`, and `OutboxPublisher` (the whole
polling/`TransactionTemplate`-per-row/retry mechanism from the Phase 3
bugfix section above) — ~700 combined lines of duplicated code across the
two services, and every future bug found in that machinery (like the two
found during Phase 3 testing) would need to be fixed twice, then a third
time in payment-service, a fourth in shipping-service. This was flagged
as worth fixing before it happened a third time, and addressed
immediately after Phase 3 rather than deferred to Phase 10 "polish".

### Why this doesn't conflict with "database per service"
Worth stating explicitly, since it might look contradictory at first: the
core guarantee of the Outbox pattern is that the domain write and the
"intent to publish" write happen in the SAME database transaction — which
only works if the outbox table lives in the SAME database as the domain
tables it's paired with. So even though the *code* (entity class,
repository interface, publisher logic) is now shared via a common Gradle
module, every service still has its OWN `outbox` table, in its OWN
database (`order_db`'s `outbox`, `inventory_db`'s `outbox`, etc.) — the
module shares behavior, not data or a transaction boundary. This is the
standard shape of the Transactional Outbox pattern in the literature
(e.g. microservices.io): one outbox table per service, not a shared
"outbox service" (which would just reintroduce the original dual-write
problem, now over the network instead of within a process).

### New module: `outbox-support`
A library module (not a runnable Spring Boot application — no
`org.springframework.boot` plugin applied, following the same shape as
the pre-existing `event-schemas` module) added to `settings.gradle.kts`,
containing:
- `persistence/OutboxEntity.java`, `OutboxStatus.java`,
  `OutboxRepository.java` — moved verbatim (byte-for-byte identical logic)
  from `order-service`.
- `application/OutboxRecorder.java` — a new `@Component` extracting what
  used to be a private `toAvroJson` + entity-building block duplicated
  inside both services' `OutboxWriter` classes. Exposes one method,
  `record(aggregateId, topic, avroRecord)`.
- `application/AbstractOutboxPublisher.java` — a new abstract class
  extracting the entire polling loop, the `TransactionTemplate`-per-row
  logic, and the Avro-JSON decode helper from what used to be two
  separate concrete `OutboxPublisher` classes. Leaves exactly one method
  abstract: `toAvroRecord(eventType, payload)`, since mapping a stored
  event-type string back to a concrete Avro class is the one piece that's
  genuinely different per service.

Each service now keeps a small concrete subclass:
```java
@Component
public class OutboxPublisher extends AbstractOutboxPublisher {
    public OutboxPublisher(OutboxRepository r, KafkaTemplate<String,Object> k,
                            PlatformTransactionManager tm) { super(r, k, tm); }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}")
    @Override
    public void publishPending() { super.publishPending(); }   // see below for why overridden here

    @Override
    protected SpecificRecord toAvroRecord(String eventType, String payload) {
        return switch (eventType) { /* this service's event types */ };
    }
}
```
and a thin `OutboxWriter` that builds its own service-specific Avro
records and delegates to `OutboxRecorder.record(...)` instead of doing
the encoding/entity-building itself.

**Why `@Scheduled` + the `publishPending()` override stays in each
subclass, not the abstract base class:** two reasons. First, the
`fixedDelayString = "${outbox.publisher.fixed-delay-ms:2000}"` SpEL
placeholder needs to resolve against each service's OWN
`application.properties` (both already define this key with the same
name, but they're independent values in independent property files —
putting `@Scheduled` on the base class would still work since Spring
resolves the placeholder against whichever service's Environment the
bean lives in, but it's clearer and less "spooky action at a distance"
to have it visible on the concrete class). Second, relying on Spring to
correctly detect and schedule an inherited (non-overridden)
`@Scheduled` method is a less commonly exercised code path than a method
declared directly on the bean's own class — overriding costs three lines
per service and removes any doubt.

### The `Persistable<UUID>` interface lives in the shared module now, `docs/decisions.md`'s Phase 2/3 explanations still apply
No behavior changed here — `OutboxEntity`'s `Persistable<UUID>` +
`@Transient boolean isNew` implementation is identical to what it was
before the move (see the Phase 2 section above for the full "why"). Only
its package changed, from `com.fabioqmarsiaj.order.persistence` /
`com.fabioqmarsiaj.inventory.persistence` to
`com.fabioqmarsiaj.outbox.persistence`.

### Required bootstrap change: explicit `@EntityScan`/`@EnableJpaRepositories`/`scanBasePackages`
This was the one genuinely tricky, non-mechanical part of the extraction.
`@SpringBootApplication` only component-scans (and, transitively via
`@EnableAutoConfiguration`'s JPA auto-configuration, entity/repository
-scans) the package the annotated class lives in, plus subpackages. Since
`com.fabioqmarsiaj.outbox` is a sibling package to
`com.fabioqmarsiaj.order`/`com.fabioqmarsiaj.inventory`, NOT a
subpackage, the default scan would never find `OutboxEntity`,
`OutboxRepository`, or the `@Component`-annotated `OutboxRecorder`/
`OutboxPublisher` subclasses after the move — the service would either
fail to start (`NoSuchBeanDefinitionException` for `OutboxRepository`)
or, worse, start but silently never create the `outbox` table.

Fixed by widening the scan explicitly on both `OrderServiceApplication`
and `InventoryServiceApplication`:
```java
@SpringBootApplication(scanBasePackages = {"com.fabioqmarsiaj.order", "com.fabioqmarsiaj.outbox"})
@EnableScheduling
@EntityScan(basePackages = {"com.fabioqmarsiaj.order.persistence", "com.fabioqmarsiaj.outbox.persistence"})
@EnableJpaRepositories(basePackages = {"com.fabioqmarsiaj.order.persistence", "com.fabioqmarsiaj.outbox.persistence"})
public class OrderServiceApplication { ... }
```
(swap `order`/`com.fabioqmarsiaj.order.persistence` for
`inventory`/`com.fabioqmarsiaj.inventory.persistence` in
`InventoryServiceApplication`). All three annotations/attributes were
needed together: `scanBasePackages` alone finds `@Component`s
(`OutboxRecorder`, the concrete `OutboxPublisher` subclass) but not JPA
entities/repositories; `@EntityScan` and `@EnableJpaRepositories` are
what Spring Boot's JPA auto-configuration actually consults for entity
classes and repository interfaces respectively — omitting either one
still breaks it, just with a different, more Hibernate/Spring-Data
-specific error further into startup.

### Gradle module-plumbing gotchas hit while creating `outbox-support`
Two errors surfaced immediately when trying to compile the new module,
both fixed and worth remembering for any future non-Spring-Boot library
module in this monorepo:
1. **`Unresolved reference 'api'`** — the `api(...)` dependency
   configuration (as opposed to `implementation(...)`) is only available
   under the `java-library` Gradle plugin, not the plain `java` plugin.
   `event-schemas` gets away with plain `java` because it only exposes
   generated Avro classes via `implementation`-declared Avro itself, but
   `outbox-support` needs `api` so consuming services see types like
   `KafkaTemplate`/`OutboxEntity` directly on their own compile
   classpath (they appear in `AbstractOutboxPublisher`'s and
   `OutboxEntity`'s own public signatures). Fixed by using the
   `` `java-library` `` plugin instead of `java`.
2. **`package jakarta.persistence does not exist`** — depending on
   `org.springframework.data:spring-data-jpa` alone (even via the Spring
   Boot BOM for version alignment) does NOT transitively pull in the JPA
   API itself (`jakarta.persistence-api`) the way
   `spring-boot-starter-data-jpa` does for an actual application module —
   the starter's own dependency graph adds it, but this module can't use
   that starter (no Spring Boot plugin applied). Fixed by adding
   `api("jakarta.persistence:jakarta.persistence-api")` explicitly,
   version-aligned via the same Spring Boot BOM import
   (`dependencyManagement { imports { mavenBom(...) } }` from the
   `io.spring.dependency-management` plugin) already used for
   `spring-data-jpa`/`spring-kafka`.

### Verification performed for this refactor
Both service test suites (Testcontainers-backed, exercising a real
Spring context against real Postgres/Kafka containers) were re-run and
passed after the extraction — this specifically exercises whether the
`@EntityScan`/`@EnableJpaRepositories` widening actually works, since a
misconfigured scan would fail context startup, not just a specific test
assertion. The full manual end-to-end walkthrough from the Phase 3
section above (seed stock, happy path reservation + decrement, rejection
path + stock left unchanged + order cancelled) was also re-run against
both `bootRun` instances after the refactor and confirmed working
identically to before.

---

## Post-Phase 3 hardening — routing Saga commands through the outbox

### The gap: `kafkaTemplate.send()` for Saga commands was fire-and-forget
`OrderCommandService` originally sent all 5 Saga commands
(`ReserveStockCommand`, `ProcessPaymentCommand`, `CreateShipmentCommand`,
`ReleaseStockCommand`, `RefundPaymentCommand`) directly via
`KafkaTemplate#send`, immediately after the `@Transactional` method that
persisted the domain event(s) which triggered them — this was called out
as a deliberate-but-risky simplification since Phase 2 (see that
section's Javadoc excerpt), but the exact failure mode was only worked
through in detail here, in a Q&A about how the pattern actually behaves.

`KafkaTemplate#send(...)` returns a `CompletableFuture<SendResult>`
without blocking — under the hood, `KafkaProducer#send` does
serialization/partitioning synchronously (inline, inside the `send()`
call) but the actual network I/O (talking to the broker, waiting for an
ack) happens asynchronously, completing the future later on a background
thread. None of `OrderCommandService`'s 6 call sites awaited or even
inspected that future (no `.join()`, `.get()`, or `.whenComplete()`).
That split created two very different failure behaviors depending on
where a send actually failed:
- **Synchronous failure** (e.g. an Avro schema registration error, like
  the Phase 3 `RecordNameStrategy` bug) — propagates immediately as an
  exception out of `.send()`, and since the enclosing method is
  `@Transactional`, Spring rolls back the WHOLE transaction (undoing the
  domain event(s) that had already been persisted too). Loud and safe:
  the client gets an HTTP 500, nothing was left half-done, and the
  request can simply be retried.
- **Asynchronous failure** (e.g. a transient broker outage, a network
  timeout) — the exception only surfaces later, on the future, which
  nothing was listening to. The `@Transactional` method has ALREADY
  returned and committed by the time the failure happens — the order's
  domain state has advanced in the database, but the command that was
  supposed to drive the Saga forward silently never left the process.
  No log, no retry, no alert: the order is just stuck in its current
  state forever, discoverable only by manually noticing it never
  progresses.

### The fix: commands go through the outbox too, reusing the exact same machinery as events
Since `OutboxEntity`/`OutboxRecorder`/`AbstractOutboxPublisher` (in
`outbox-support`) were already generic over an arbitrary `(aggregateId,
topic, SpecificRecord)` triple — with no assumption baked in that the
`SpecificRecord` has to be an "event" rather than a "command" — closing
this gap required no schema change and no new infrastructure, only
routing `OrderCommandService`'s command-sending call sites through the
outbox instead of a direct `KafkaTemplate` call:
- `OutboxWriter` (order-service) gained one new method,
  `writeCommand(orderId, topic, command)`, a thin wrapper around
  `OutboxRecorder.record(...)` — symmetric to the pre-existing `writeAll`
  (which handles the 4 `Order*` integration events).
- `OrderCommandService`'s `KafkaTemplate` dependency was removed
  entirely; each of the 6 call sites now calls a private
  `recordCommand(orderId, topic, command)` helper instead, which just
  delegates to `outboxWriter.writeCommand(...)` — still inside the same
  `@Transactional` method, so the command's "intent to send" is recorded
  atomically with the domain event(s) that triggered it, exactly like
  events already were.
- `OutboxPublisher` (order-service) gained 5 more cases in its
  `toAvroRecord` switch (`ReserveStockCommand`, `ReleaseStockCommand`,
  `ProcessPaymentCommand`, `RefundPaymentCommand`,
  `CreateShipmentCommand`) — no other change; the exact same polling
  loop, per-row `TransactionTemplate`, synchronous `.join()` send, and
  retry-on-next-poll behavior that was already handling `order.events`
  now also handles `inventory.commands`/`payment.commands`/
  `shipping.commands`. One `outbox` table, one publisher, one poll loop,
  now carrying both events and commands — the `topic` column (already
  present on every row since Phase 2) is what routes each row to the
  right destination; nothing in the publishing mechanism needed to know
  or care about the event/command distinction.

This closes the async-failure gap the same way it was already closed for
integration events: a row only ever gets marked `PUBLISHED` after
`AbstractOutboxPublisher`'s synchronous `.join()` confirms the Kafka send
actually succeeded; if it doesn't, the row's own transaction rolls back,
it stays `PENDING`, and the next poll (2s later, by default) retries it
automatically — no manual intervention, no silent data loss.

### Manually verified
`POST /orders` against a freshly-seeded stock level, watching both
services' logs and querying the `order_db.outbox` table directly:
```
event_type             | topic               | status
OrderCreated           | order.events        | PUBLISHED
ReserveStockCommand    | inventory.commands  | PUBLISHED
ProcessPaymentCommand  | payment.commands    | PUBLISHED
```
`ReserveStockCommand` and `ProcessPaymentCommand` now go through the same
`outbox` table and `PENDING` → `PUBLISHED` lifecycle as `OrderCreated` —
confirming inventory-service still receives and reacts to
`ReserveStockCommand` correctly (it published `StockReserved` in
response, which is only reachable by first having consumed the command),
and the Saga continues to advance exactly as it did with the old
direct-`KafkaTemplate` approach, just without the async-failure gap.

### Scope: order-service only
inventory-service is a Saga *participant*, not the orchestrator — it has
never sent commands (only events, which already went through the outbox
since Phase 3), so no change was needed there. The same fix should be
applied proactively in payment-service/shipping-service if either of
them ever needs to send an outgoing command of their own (none currently
do — both are terminal participants in their part of the Saga, only ever
replying with events, never issuing further commands downstream).

---

## Cross-cutting notes for Phase 4+ (payment-service, shipping-service, and beyond)

Things established in Phases 2-3 (and the post-Phase-3 refactor) that
will likely recur:

- **Depend on `outbox-support` (`implementation(project(":outbox-support"))`),
  don't reimplement the outbox.** payment-service and shipping-service
  should each get a thin `OutboxWriter` (building their own Avro records,
  delegating to `OutboxRecorder.record(...)`) and a thin
  `OutboxPublisher extends AbstractOutboxPublisher` (just the
  `@Scheduled` override + a `toAvroRecord` switch), exactly like
  order-service/inventory-service — see the "Post-Phase 3 refactor"
  section above for the full shape.
- **Remember the `@EntityScan`/`@EnableJpaRepositories`/`scanBasePackages`
  widening** on each new service's `@SpringBootApplication` class — see
  above for the exact annotations needed and why; easy to forget since
  the service will compile fine and only fail (or worse, silently
  misbehave) at runtime without it.
- Reuse the `Persistable<UUID>` / `Persistable<String>` pattern for any
  entity whose `@Id` is never null at construction time — whether an
  app-assigned UUID or a natural/business key.
- Reuse `columnDefinition = "TEXT"` instead of `@Lob` for any JSON string
  payload column on PostgreSQL.
- Remember Jackson 3 (`tools.jackson.databind.ObjectMapper`) if any service
  needs to hand-serialize domain events to JSON like `OrderEventMapper` does.
- Remember the Avro JSON codec (`EncoderFactory`/`DecoderFactory` +
  `SpecificDatumWriter`/`SpecificDatumReader`) is already handled for you
  inside `outbox-support`'s `OutboxRecorder`/`AbstractOutboxPublisher` —
  no need to hand-roll it again for payment-service/shipping-service's
  own outbox.
- **Set `spring.kafka.producer.properties.value.subject.name.strategy=
  io.confluent.kafka.serializers.subject.RecordNameStrategy` from the
  START** for any service producing to a topic that carries more than one
  event type (which is every `*.events`/`*.commands` topic in this
  project) — don't wait to rediscover the Phase 3 Schema Registry bug
  again.
- Any Saga replies (`*.events` topics) payment-service/shipping-service
  publish should go through an outbox from the start, same as
  order-service's `order.events` and inventory-service's
  `inventory.events` already do. (The historical gap where
  order-service's outgoing Saga *commands* bypassed the outbox — see
  Phase 2's original Javadoc excerpt — has since been closed; see
  "Post-Phase 3 hardening — routing Saga commands through the outbox"
  above. If payment-service/shipping-service ever need to send their own
  outgoing commands, route them through the outbox from the start rather
  than repeating that now-fixed mistake.)
- Pick a `server.port` for each new service before running multiple
  services simultaneously outside Docker (order-service = 8082,
  inventory-service = 8083; payment-service and shipping-service should
  take 8084 and 8085 respectively).
- If a reservation/decrement-style operation needs "all or nothing"
  semantics across multiple rows in one command (like inventory-service's
  stock reservation), consider the atomic-conditional-UPDATE +
  in-transaction-compensation pattern documented above rather than
  optimistic locking with a retry loop.
- If a new library module is needed (like `outbox-support`), remember it
  needs `` `java-library` `` (not plain `java`) to use `api(...)`
  dependencies, and may need dependencies explicitly declared that a
  Spring Boot starter would normally provide transitively (e.g.
  `jakarta.persistence-api`) — see the Gradle gotchas above.
