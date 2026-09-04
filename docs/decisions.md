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

## Phase 4 — payment-service

### `PaymentEntity`: load-bearing state, not just an audit trail
Unlike inventory-service's `stock_reservations` table (pure audit — the
compensating `ReleaseStockCommand` already carries every line item
needed, so nothing actually depends on reading that table back),
payment-service's `payments` table is load-bearing. The
`RefundPaymentCommand` Avro schema carries only `orderId` and
`amountCents` — NOT the `paymentId` that payment-service itself generates
when a payment is approved (`PaymentApproved.paymentId`). Without
persisting that `paymentId` (and the fact that a payment was ever
approved for this order in the first place) somewhere, a later refund
command would have no way to know whether refunding is even valid, or
what payment it's supposedly refunding.

`PaymentEntity` is keyed by `orderId` directly (a natural key — at most
one payment attempt per order in this simplified simulation), same
`Persistable<UUID>` + `@Transient isNew` treatment as every other
non-null-at-construction `@Id` in this project. Both `APPROVED` and
`DECLINED` outcomes get a row (not just approvals) — needed for the
`GET /payments/{orderId}` endpoint to give a complete picture, and
because `DECLINED` rows are only ever read, never looked up as the
target of a future refund (a refund command for a `DECLINED`/nonexistent
payment is handled defensively — see below — so persisting `DECLINED`
rows doesn't complicate that check, it just makes the read side more
useful).

### Approval rule is a configurable amount threshold, not hardcoded
`payment.approval.limit-cents` in `application.properties`
(`PaymentCommandService` reads it via `@Value`) — amounts above the limit
are declined, everything else approved. Deliberately simple (this is a
simulation, not a real payment gateway integration), but configurable
specifically so the decline path can be forced during manual testing by
temporarily lowering the limit, without recompiling — the same
"observable both ways" testing approach used for inventory-service's
insufficient-stock rejection path in Phase 3.

### Refund for a payment that isn't `APPROVED`: log and skip, don't publish
`PaymentCommandService.handleRefundPayment` looks up the `PaymentEntity`
by `orderId` and, if it's missing OR not currently `APPROVED` (e.g.
already `REFUNDED`, or the payment was actually `DECLINED` and this
refund command shouldn't logically exist), logs a warning and returns
without persisting anything or writing to the outbox. This is a
defensive branch, not expected to trigger given the Saga's design — but
Kafka only guarantees at-least-once delivery, so a duplicate or
theoretically out-of-order redelivery isn't impossible. The alternative
(publish `PaymentRefunded` anyway, "best effort") was considered and
rejected: it would mean order-service could receive a refund
confirmation for a payment that was never actually taken, which is worse
than simply not confirming a refund that shouldn't have been requested.

---

## Post-Phase 4 fix — every Kafka event type on a topic needs its own `@KafkaHandler`

### The bug: `KafkaException: No method found`, and a permanently wedged partition
Discovered live during Phase 4 manual testing — the FIRST time the Saga's
compensation path (payment declined → release stock) was ever actually
exercised end-to-end (inventory/payment-service didn't exist yet when
this path was written in Phase 2, so it had never been run for real).
The moment inventory-service published a real `StockReleased` event,
order-service's `InventoryEventListener` blew up:
```
org.springframework.kafka.KafkaException: No method found for class com.fabioqmarsiaj.events.inventory.StockReleased
```
followed by the container endlessly re-seeking to the same offset
("Record in retry and not yet recovered") until backoff attempts were
exhausted, at which point the whole partition for that consumer group
was stuck — no further messages on that partition would ever be
processed until the process was fixed and restarted.

The root cause: `InventoryEventListener`'s class-level `@KafkaListener` +
method-level `@KafkaHandler` combination had `@KafkaHandler` methods for
`StockReserved` and `StockRejected`, but NOT `StockReleased` — the third
event type `inventory.events` carries. The class's own Javadoc, written
in Phase 2, incorrectly claimed this was safe: *"A message type without a
matching handler ... is simply not delivered to this listener."* That
claim was never actually tested until now, and it's wrong: Spring Kafka's
`DelegatingInvocableHandler` throws when it can't find a matching method,
it doesn't silently skip the message. Without a message-specific error
handler configured to skip-and-continue, the container's default
behavior for a listener exception is to retry the SAME record
indefinitely (with backoff) — which is the correct, safe default for
genuinely transient failures, but catastrophic for what is actually a
permanent "this type will never be handled" programming error: the
record can never succeed no matter how many times it's retried, so the
partition is effectively dead until a human intervenes.

`PaymentEventListener` had the identical latent bug for
`PaymentRefunded` (the third event type on `payment.events`) — not yet
triggered live only because shipping-service (Phase 5) doesn't exist
yet, so `handleShipmentFailed` (the only path that sends
`RefundPaymentCommand`) was never reachable end-to-end. Fixed
proactively alongside the `StockReleased` fix rather than waiting to
rediscover the same bug in Phase 5.

### The fix: a `@KafkaHandler` for every publishable event type, even if it's just a log line
Both listeners gained a handler for their previously-missing third event
type. Since order-service genuinely has nothing further to DO in
reaction to a compensation acknowledgment (the order is already in a
terminal state — `CANCELLED`/`FAILED` — by the time `StockReleased`/
`PaymentRefunded` arrives), the new handlers are intentionally minimal —
just consume the message and log that the Saga ends here for this
branch, which is exactly what's needed to let the partition offset
advance normally.
```java
@KafkaHandler
public void onStockReleased(StockReleased event) {
    UUID orderId = UUID.fromString(event.getOrderId());
    log.info("Order {}: stock released (compensation ack) - Saga ends here for this branch", orderId);
}
```
Both classes' Javadoc was rewritten to describe the ACTUAL Spring Kafka
behavior (throws and wedges the partition, doesn't silently skip) and to
state explicitly, as a rule for future maintainers: **every publishable
event type on a multi-type topic needs a matching `@KafkaHandler` on
every listener of that topic, with no exceptions** — even a
"deliberately ignored" event type needs an explicit (if trivial) handler,
never an absent one.

### General lesson for Phase 5+ (and any future `@KafkaListener` on a multi-type topic)
Before adding or reviewing any class-level `@KafkaListener` +
`@KafkaHandler` listener, cross-check its topic's FULL set of publishable
Avro types (consult the relevant `event-schemas` directory and every
service that publishes to that topic) against the listener's
`@KafkaHandler` methods — a compile-time-invisible gap here doesn't fail
loudly until the exact message type that's missing a handler actually
gets published for the first time, which as this bug shows can be a long
time after the listener was originally written and believed to be
correct/tested.

---

## Phase 5 — shipping-service

### No natural business field to key a success/failure simulation on
Unlike inventory-service (checks actual stock quantity from
`ReserveStockCommand.items`) and payment-service (compares
`ProcessPaymentCommand.amountCents` against a configurable threshold),
`CreateShipmentCommand`'s schema carries only `eventId`/`orderId`/
`customerId`/`occurredAt` — no amount, no items, no address, nothing
resembling a business quantity a "should this fail" rule could
plausibly hang off of. Confirmed by checking both the Avro schema and
`order-service`'s `SagaCommandFactory#createShipment`, which deliberately
builds the command from just `order.getId()`/`order.getCustomerId()`
(unlike `reserveStock`/`releaseStock`, which do include line items, or
`processPayment`/`refundPayment`, which do include the amount).

**Fix: a sentinel `customerId` forces a simulated failure.**
`shipping.simulation.force-fail-customer-id` in `application.properties`
(default `fail-customer`) — `ShippingCommandService` compares the
incoming command's `customerId` against this value; an exact match
simulates `ShipmentFailed`, everything else simulates `ShipmentCreated`.
This gives the same testing ergonomics as inventory-service's
"request more than available" and payment-service's "request above the
limit": deterministic, per-request control over which path executes
(just supply the sentinel `customerId` in `POST /orders`), no service
restart needed to switch between testing the happy path and the
compensation path. Two alternatives were considered and rejected: a
random failure-rate percentage (not deterministic — could require
several attempts to actually hit the failure path during a manual test)
and a global boolean config flag (affects every order uniformly, would
require restarting the service to toggle between testing scenarios).

### `ShipmentEntity` is audit/query only, not load-bearing — same role as inventory-service's `stock_reservations`
Contrast with payment-service's `PaymentEntity`, which is genuinely
load-bearing (a later `RefundPaymentCommand` needs to look up the
`paymentId` generated on approval, since the command itself doesn't
carry it back). There is no compensating command that flows INTO
shipping-service at all — no `CancelShipmentCommand` exists in this
Saga's design. A shipment failure is terminal and handled entirely by
order-service compensating payment/inventory instead (see
`OrderCommandService#handleShipmentFailed`, which sends both
`RefundPaymentCommand` and `ReleaseStockCommand`, never anything back to
shipping-service). So `ShipmentEntity` exists purely so
`GET /shipments/{orderId}` has something to return, and as the natural
place to hang future idempotent-consumption logic (deferred, same as
every other service's P1 "Idempotent consumption" item) — nothing in
the Saga's correctness depends on this table.

### Milestone: the full Saga was manually verified end-to-end for the first time
With all four participant services now implemented, both remaining
untested Saga paths were exercised for real:
- **Full happy path**: `POST /orders` → `STOCK_RESERVED` → stock
  decremented → `PAYMENT_APPROVED` → `ShipmentCreated` published →
  order reaches `COMPLETED`. This is the first time an order has ever
  reached its terminal success state in this project.
- **Shipment failure → dual compensation**: `POST /orders` with the
  sentinel `customerId` → order proceeds normally through stock
  reservation and payment approval, then `ShipmentFailed` is published →
  `order-service`'s `handleShipmentFailed` fires BOTH
  `RefundPaymentCommand` and `ReleaseStockCommand` (via the outbox, one
  transaction, per the post-Phase-3 hardening) → payment-service marks
  the payment `REFUNDED` → inventory-service releases the stock back →
  order reaches `FAILED`. Confirmed directly in each service's database:
  `payment_db.payments.status = REFUNDED` and `inventory_db.stock`
  restored to its pre-reservation quantity. This is the first time two
  independent compensating actions have been triggered from a single
  domain event and both verified to complete correctly.

This closes out everything Phase 6 ("End-to-end Saga") had listed as
P0 except documenting Mermaid sequence diagrams — the actual
verification work is done; Phase 6 as a checklist item is now mostly a
documentation pass, not new testing.

---

## Phase 7 — query-service (Part A: read model)

query-service is architecturally different from every service built so
far: it's a pure CQRS read-model consumer, not a Saga participant. It
never publishes domain events, never sends Saga commands, and — unlike
every other service — does NOT depend on `outbox-support` at all (that
module's entire public API, `OutboxRecorder`/`AbstractOutboxPublisher`,
is producer-side only; confirmed by inspection before starting this
phase). Its `application.properties` has no `[Producer]` section, no
`KafkaTemplate` bean is ever injected anywhere in this service, and its
`@SpringBootApplication` needs no `scanBasePackages`/`@EntityScan`/
`@EnableJpaRepositories` widening either — since it doesn't reach into
`com.fabioqmarsiaj.outbox` (a sibling package), all of its own classes
already live under its own base package, which Spring's default
component/entity scan already covers.

### Two read models, deliberately kept separate
- **`TimelineEntryEntity`** (table `timeline_entries`) — append-only, one
  row per domain event consumed across all FOUR `*.events` topics
  (`order.events`, `inventory.events`, `payment.events`,
  `shipping.events`). Structurally similar to order-service's
  `OrderEventEntity` (same `Persistable<UUID>`, always-`isNew()`,
  `columnDefinition = "TEXT"` payload shape) but conceptually different:
  it has no authority of its own — it's a derived, denormalized,
  cross-service view that could be dropped and rebuilt from scratch by
  replaying the four topics from the beginning at any time (the
  defining property of a CQRS read model, unlike an event-sourced
  aggregate's own event store).
- **`OrderSummaryEntity`** (table `order_summaries`, natural key
  `orderId`) — one row per order, powering `GET /orders?status=`.
  Projected EXCLUSIVELY from `order.events`, never from the other three
  topics. This was a deliberate choice over a richer "live status" that
  also promotes through `STOCK_RESERVED`/`PAYMENT_APPROVED` by listening
  to `inventory.events`/`payment.events` too: since Kafka topics are
  independent of each other, an event on e.g. `inventory.events` could
  theoretically be consumed and processed before the corresponding
  `OrderCreated` on `order.events` — projecting status from a SINGLE
  topic (partitioned/ordered per `orderId`, like every topic in this
  project) sidesteps that race entirely, at the cost of
  `OrderSummaryStatus` only ever being able to represent
  `CREATED`/`COMPLETED`/`CANCELLED`/`FAILED` — the four types
  `order.events` actually carries — never the three intermediate
  transitions. Those three ARE still fully visible, just via the
  granular `TimelineEntryEntity`/`GET /orders/{id}/timeline` instead.

### Avro payload → JSON payload, NOT via Avro's own JSON codec this time
Every producer-side outbox writer in this project (order/inventory/
payment/shipping-service, via `outbox-support`'s `OutboxRecorder`) uses
Avro's own schema-aware JSON codec (`EncoderFactory`/`SpecificDatumWriter`)
specifically because the stored payload later needs to be decoded back
into a strongly-typed Avro `SpecificRecord` for republishing — and plain
Jackson can't handle Avro's `getSchema()` bean-style getter. `query-service`'s
`TimelineRecorder` is different: it never needs to reconstruct an Avro
object from what it stores — the timeline API only ever deserializes
the payload back into a generic `Map<String,Object>` (see
`TimelineEntryResponse.from`). So `TimelineRecorder` builds a plain
`Map<String,Object>` of just the fields worth showing (e.g.
`Map.of("reason", event.getReason())`) in each listener, and serializes
THAT with plain Jackson 3 (`tools.jackson.databind.ObjectMapper`, same
Spring Boot 4 default noted since Phase 2) — no Avro codec involved at
all on this side, since the Avro object itself is never what's being
stored.

### Manually verified end-to-end (Part A)
Ran all 5 services together (order/inventory/payment/shipping/query)
against real traffic from the already-established Saga scenarios:
- **Happy path**: `POST /orders` → `GET /orders/{id}/timeline` on
  query-service showed the full cross-topic event sequence
  (`OrderCreated` → `StockReserved` → `PaymentApproved` →
  `ShipmentCreated` → `OrderCompleted`); `GET /orders?status=COMPLETED`
  correctly listed the order.
- **Failure/compensation path**: verified similarly — the timeline
  correctly showed the compensation events (e.g. `PaymentDeclined`/
  `StockReleased`/`OrderCancelled`, or the shipment-failure variant with
  both `RefundPaymentCommand`'s resulting `PaymentRefunded` and
  `ReleaseStockCommand`'s resulting `StockReleased`), and
  `GET /orders?status=` correctly reflected the terminal
  `CANCELLED`/`FAILED` status.

### Part B (Kafka Streams analytics) deferred to its own pass
`query-service/build.gradle.kts` already had a bare
`org.apache.kafka:kafka-streams` dependency pre-scaffolded (from initial
repo setup, unused until now). Part A doesn't touch it — the Kafka
Streams topology over `user-activity.events` (P1 in TASKS.md) and its
`GET /analytics/top-products` endpoint are implemented separately, since
there's no producer of `user-activity.events` yet (`ingestion-service`
is Phase 8) — Part B can be implemented and will compile, but can't be
manually verified with real traffic until Phase 8 exists. See the
follow-up entry below once Part B lands.

---

## Phase 7 — query-service (Part B: Kafka Streams analytics)

### No Spring Boot Kafka Streams starter needed — `spring-boot-starter-kafka` already brings it
Before writing any topology code, it's worth recording what was
confirmed by inspecting the actual dependency jars (not assumed from
memory/tutorials, which mostly predate Spring Boot 4's package
reorganization): `spring-boot-starter-kafka` (already a dependency of
every service in this project, including query-service) transitively
pulls in `spring-kafka`, which already provides `@EnableKafkaStreams`,
`KafkaStreamsConfiguration`, and `StreamsBuilderFactoryBean` — no
additional `spring-kafka` dependency needed. More surprisingly, Spring
Boot's own `spring-boot-kafka` autoconfiguration module
(`org.springframework.boot.kafka.autoconfigure.KafkaStreamsAnnotationDrivenConfiguration`)
is *conditionally* active too: it's `@ConditionalOnBean(name =
"defaultKafkaStreamsBuilder")` — the exact bean name
`@EnableKafkaStreams` registers — and, once active, automatically builds
the required `KafkaStreamsConfiguration` bean by reading
`spring.kafka.streams.*` properties (`application-id`, and everything
under `spring.kafka.streams.properties.*` as raw Kafka Streams config).
This means the ONLY code needed to wire up Kafka Streams here is
`@Configuration @EnableKafkaStreams` on one class — no manual `new
KafkaStreamsConfiguration(Map.of(...))` bean, unlike what most
older/pre-Spring-Boot-4 Kafka Streams tutorials show. Only
`io.confluent:kafka-streams-avro-serde:7.6.13` needed to be added as a
new dependency (same version already used for `kafka-avro-serializer`
everywhere else in this project — confirmed to exist in the Confluent
Maven repo before using it).

### `user-activity.events` read as `GenericRecord`, filtered by schema name — not `SpecificAvroSerde`
Same multi-type-topic shape as every other topic in this project:
`user-activity.events` carries three unrelated Avro types
(`ProductViewed`/`AddedToCart`/`SearchPerformed`). A Kafka Streams
topology's default value serde has to be ONE serde for the whole
topology, so `SpecificAvroSerde<ProductViewed>` isn't an option here the
way a per-message-type `@KafkaHandler` is for `@KafkaListener`-based
consumers. Instead, the default value serde is Confluent's
`GenericAvroSerde` (schema-aware, but deserializes into a generic
`org.apache.avro.generic.GenericRecord` rather than a specific generated
class), and the topology's first step filters down to just
`ProductViewed` by checking `record.getSchema().getName()` — the
Kafka-Streams-side equivalent of the `eventType` discriminator string
pattern used everywhere else in this project (outbox rows,
order-service's event store, this same service's own
`timeline_entries` table from Part A).

### Interactive query: current window only, via `StreamsBuilderFactoryBean` + `ReadOnlyWindowStore`
`GET /analytics/top-products` queries ONLY the current (most recent,
still-open) 1-minute tumbling window — the more literal reading of
TASKS.md's wording ("tumbling window (1 min) ... count via state
store"), simpler than summing several recent windows, and consistent
with treating this as a lightweight, best-effort analytics feature
rather than a precise historical report. `TopProductsQueryService`
injects the `StreamsBuilderFactoryBean` (the same bean
`@EnableKafkaStreams` registers), pulls the live `KafkaStreams` instance
out of it, and queries the named `product-view-counts` state store via
`KafkaStreams#store(...)` + `ReadOnlyWindowStore#fetchAll(windowStart,
windowEnd)` — the window boundaries are computed by truncating "now"
down to the configured window size, mirroring exactly how Kafka Streams
itself assigns records to tumbling windows. Returns an empty list if the
store isn't ready yet or nothing has been counted in the current window
— expected/normal state until ingestion-service (Phase 8) actually
produces `ProductViewed` traffic, not a bug.

### Bug found via query-service's own test suite: Kafka Streams needs its source topic to already exist, unlike `@KafkaListener`
This is the one part of Part B that was actually manually verified
(indirectly) before Phase 8 exists: re-running query-service's existing
`ApplicationTests` (unchanged, still just `contextLoads()`) after adding
the Streams topology revealed a real difference in failure behavior
between `@KafkaListener`-based consumers and Kafka Streams. Every other
consumer in this project (all the `@KafkaListener` classes across every
service) tolerates a topic not existing yet — it just waits/retries. A
Kafka Streams topology does NOT: with `allow.auto.create.topics=false`
(the correct, deliberate setting used everywhere in this project — see
earlier phases), a topology whose SOURCE topic doesn't exist throws
`MissingSourceTopicException` and leaves the entire `StreamThread`
**permanently** in the `ERROR` state — not a transient condition that
resolves once the topic is created later, unlike what the log's own
wording ("a new rebalance will be kicked off automatically") suggests
happens for other kinds of missing-metadata issues. This surfaced
because query-service's Testcontainers-managed test broker starts
completely empty (no topics pre-created, unlike the real
`docker-compose` broker where `infra/kafka/init-topics.sh` already
creates all 8 topics) — so every test run was silently leaving the
Streams thread broken, without failing the actual JUnit assertion
(`contextLoads()` only checks the Spring context starts, not that every
bean is healthy).

**Fix:** declared `user-activity.events` as a Spring-managed `NewTopic`
bean (`TopicBuilder.name(...).partitions(3).replicas(1).build()`) in
`UserActivityStreamsConfig`. Spring Boot's `KafkaAdmin` auto-configuration
picks up every `NewTopic` bean in the context and idempotently ensures
it exists before the rest of the context (including the Streams thread)
starts — a no-op against the real broker (topic already exists there),
but exactly what the empty test broker needed. Re-ran the test suite
after the fix and confirmed via the raw log output that the
`StreamThread` now reaches `RUNNING` (previously: permanently `ERROR`,
with the build still reporting `BUILD SUCCESSFUL`/no test failures,
since nothing was asserting on Streams health specifically). Worth
remembering for Phase 8 (ingestion-service) and any future service that
adds its own Kafka Streams topology: **always declare a `NewTopic` bean
for every source topic a topology reads from**, even though no other
`KafkaTopics` class in this project has ever needed to do this for
plain `@KafkaListener` consumers.

---

## Phase 8 — ingestion-service (Go)

### Library choices: hamba/avro/v2, segmentio/kafka-go, net/http, no cgo
The Phase 0 scaffold had picked no libraries yet, only `go.mod` +
`internal/config`. Three choices worth recording, each verified against
the actual package docs before committing to it (not from memory/habit):
- **`hamba/avro/v2`** for Avro, not `goavro` (the more historically
  well-known option) — `goavro`'s own README states LinkedIn (its
  maintainer) has internally moved to `hamba/avro` for performance and
  no longer actively develops `goavro`. `hamba/avro` also supports typed
  Go structs with `avro:"..."` tags (including automatic
  `long`/`timestamp-millis` <-> `time.Time` conversion), letting
  `internal/schema`'s `ProductViewed`/`AddedToCart`/`SearchPerformed`
  structs mirror their `.avsc` fields directly — `goavro` only ever
  decodes into `map[string]interface{}`, needing more manual code.
- **`segmentio/kafka-go`**, not `confluent-kafka-go` — the latter is a
  cgo wrapper around the C library `librdkafka`, requiring a working C
  toolchain to build; `kafka-go` is pure Go, avoiding any such
  complication on this Windows dev machine.
- **`net/http` alone**, no router library (e.g. `chi`) — `internal/httpapi`
  only ever needs two routes (`GET /health`, `GET /stats`), for which
  Go 1.22+'s built-in `http.ServeMux` method-pattern routing
  (`mux.HandleFunc("GET /health", ...)`) is already sufficient.

### The 3 user-activity.events Avro schemas are duplicated into the Go module, not shared
`event-schemas/src/main/avro/useractivity/*.avsc` (the canonical
schemas every Java service ultimately generates its Avro classes from)
live outside `ingestion-service`'s own module tree, and Go's
`go:embed` can only embed files within the importing package's own
module — there's no equivalent to Gradle's cross-module
`project(":event-schemas")` dependency here. Rather than reading the
files from a relative filesystem path (which would only work if the
binary is always run from a fixed location relative to the monorepo
root — fragile, especially for any future containerized deployment),
the 3 `.avsc` files were copied verbatim into
`ingestion-service/internal/schema/avro/` and embedded via `go:embed`.
This is a deliberate, documented duplication, not an oversight: if the
upstream schemas in `event-schemas/` ever change, these 3 copies need
updating by hand to match. Since `user-activity.events`' schemas have
been stable since Phase 0 and have no consumer-side compatibility
concerns of their own (Schema Registry is still the actual source of
truth at runtime — see below), this tradeoff was accepted for
simplicity over introducing any kind of build-time schema-sync step.

### Schemas are registered with Schema Registry at startup, using RecordNameStrategy — same as every Java producer
`internal/schema.Registrar.RegisterAll` calls Schema Registry's
`CreateSchema` (idempotent — registering an already-registered,
unchanged schema just returns its existing ID) once per event type at
process startup, before the worker pool starts publishing — a
config/connectivity problem with Schema Registry then fails loudly and
immediately (`log.Fatalf`), not confusingly deep inside a worker
goroutine's first publish attempt.

Each event type is registered under its own subject, named after the
Avro record's fully-qualified name (e.g.
`com.fabioqmarsiaj.events.useractivity.ProductViewed`) — this is
`RecordNameStrategy`, matching every other multi-type topic in this
project (`order.events`, `inventory.events`, etc. — see Phase 3's
"Multi-type Kafka topics break Schema Registry compatibility checks
under the default subject strategy"). `user-activity.events` is
exactly this shape: 3 structurally unrelated record types sharing one
topic. Verified live: `GET http://localhost:8081/subjects` after
running ingestion-service shows all 3
`com.fabioqmarsiaj.events.useractivity.*` subjects registered alongside
every existing Java-service subject.

### Hand-rolled Confluent wire-format encoding (magic byte + schema ID + Avro binary)
`hamba/avro/v2` encodes/decodes raw Avro binary; it does not itself
implement Confluent's wire format (the extra framing every Java service
in this project already produces/consumes via `KafkaAvroSerializer`/
`KafkaAvroDeserializer`, and that `query-service`'s `GenericAvroSerde`
expects to read). `internal/producer/encode.go`'s
`EncodeConfluentWire` does this by hand: 1 magic byte (`0x0`) + a
4-byte big-endian schema ID + the Avro binary payload — matching the
format documented at
https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format.
Verified via a round-trip unit test (`encode_test.go`, no live Schema
Registry needed — it parses the embedded schema JSON directly and
decodes the produced wire bytes back into the original struct) and,
more importantly, live: `query-service`'s existing
`KafkaAvroDeserializer`/`GenericAvroSerde` machinery (Phase 7)
successfully deserialized real messages produced by ingestion-service
with no changes needed on the consumer side, confirming the hand-rolled
encoder produces byte-for-byte-compatible output.

### Session-correlated funnel generation, not independent random events
`internal/activity.Generator.GenerateSession` deliberately models one
simulated *session* at a time (search -> view 1-3 products -> maybe add
one viewed product to cart), all sharing one `sessionId`, rather than
generating each event type independently at random. Two reasons this
matters:
1. **Realism** — a real shopper's activity is causally connected
   (you only add to cart something you viewed), which a purely
   independent per-event generator can't represent (nothing would stop
   it from emitting an `AddedToCart` for a product that was never
   `ProductViewed` in the same session).
2. **Interesting analytics output** — `query-service`'s "top viewed
   products" Kafka Streams topology (Phase 7 Part B) benefits from
   input that has actual funnel shape (views >> carts, weighted product
   popularity) rather than a flat 1/3 split across event types, which
   would make the resulting counts look like noise.

Weights (`searchProbability = 0.40`, `addToCartProbability = 0.20`,
1-3 `ProductViewed` per session) were chosen to approximate a plausible
e-commerce funnel, not derived from any real data — this is simulated
traffic. `sessionId` doubles as the Kafka message key (`Publisher`'s
`keyOf`), keeping every event from one session on the same partition
and in relative order, the same "key by the aggregate the events are
about" convention used for every other topic in this project (orderId,
productId, etc.).

### Fixed, self-contained fake product catalog — deliberately not sourced from inventory-service
`internal/activity/catalog.go` hardcodes ~25 fake product IDs
(`sku-1`..`sku-25`). inventory-service's real `productId`s are created
ad hoc via `POST /stock` during manual testing, with no fixed list or
listing endpoint to query — coupling ingestion-service to
inventory-service's runtime state would add a real dependency this
simulation doesn't need (Phase 7 Part B's analytics only cares about
counting views per `productId` string, not about that product actually
existing in a Saga). Kept the two completely decoupled on purpose.

### No outbox/retry for ingestion-service's publishes — an intentional asymmetry with the rest of the project
Every other Kafka producer in this project (order-service,
inventory-service, payment-service, shipping-service) routes every
publish through the Outbox pattern (`outbox-support`) for
crash-safe, at-least-once delivery — because those events/commands
drive the Saga forward, and losing one silently corrupts a real
business process. ingestion-service has no such requirement: its
output is simulated, best-effort traffic with no downstream Saga
participation and no domain state of its own to keep consistent with
an "intent to publish". `producer.Publisher.Publish` simply returns
its error to the caller; `internal/worker.Pool.runOne` logs it and
increments `stats.Counters.publishErrors`, then moves on to the next
event — no retry, no outbox row, no dead-letter handling. This is a
deliberate, documented choice, not an oversight inconsistent with the
rest of the project's at-least-once guarantees.

### Aggregate (not per-worker) rate limiting via a single shared golang.org/x/time/rate.Limiter
`config.Config.EventsPerSecond` (default 50) is documented in
TASKS.md as simulating "high volume" overall, not per goroutine — so
`worker.Pool` creates exactly one `rate.Limiter`, shared across all
`WorkerCount` (default 4) goroutines, rather than one limiter per
worker (which would have made the real aggregate throughput
`WorkerCount * EventsPerSecond`, silently decoupling the env var's
documented meaning from its actual effect). Burst is set equal to the
rate itself, allowing up to one second's worth of events to fire back
to back after any idle period — a reasonable default with no external
SLA to honor more precisely.

### Manually verified end-to-end, including a real bug found in query-service by real traffic
Ran standalone first: `go run ./cmd/ingestion-service` against the
Phase 1 Docker Compose stack (no other service running) confirmed
`GET /health` returns 200, `GET /stats` shows growing per-type counters
matching Kafka UI's view of `user-activity.events`' partition offsets
exactly (24 messages published in ~6s at `EVENTS_PER_SECOND=20` matched
`kafka-get-offsets.sh`'s reported total across all 3 partitions,
byte-for-byte), and Schema Registry's `/subjects` endpoint shows all 3
new subjects registered under `RecordNameStrategy` alongside every
existing Java-service subject.

Running `query-service` (`:query-service:bootRun`) at the same time
against this real traffic — the very first time Phase 7 Part B's Kafka
Streams topology ever processed a real `user-activity.events` message,
since Part B had previously only ever been verified by compilation
(see "Phase 7 — query-service (Part B: Kafka Streams analytics)" above)
— immediately surfaced a real bug: `UserActivityStreamsConfig`'s
`.map((key, value) -> KeyValue.pair((String) value.get("productId"), value))`
threw `ClassCastException: class org.apache.avro.util.Utf8 cannot be
cast to class java.lang.String` on every single message. Root cause: a
`GenericRecord`'s `get(fieldName)` returns Avro's own
`org.apache.avro.util.Utf8` wrapper type for Avro `"string"` fields by
default, not `java.lang.String` — a direct `(String)` cast compiles
fine (the cast is unchecked at compile time) but always fails at
runtime once real data flows through. Fixed by calling `.toString()`
instead of casting (works for both `Utf8` and, defensively, `String`
itself, so it stays correct even if the value serde's string handling
ever changes) — see `UserActivityStreamsConfig.java`'s inline comment
for the fix itself. After the fix, `GET /analytics/top-products`
against `query-service` returned real, non-empty
`{productId, viewCount}` results for the first time, confirming the
full pipeline (ingestion-service -> user-activity.events -> Kafka
Streams tumbling-window count -> interactive query -> REST API) works
end to end. This is exactly the kind of gap "verification by
compilation only" (Phase 7 Part B's explicitly accepted limitation)
couldn't have caught — a good illustration of why that limitation was
called out at the time rather than silently assumed to be fine.

---

## Cross-cutting notes for Phase 5+ (shipping-service, query-service, and beyond)

Things established in Phases 2-4 (and the post-Phase-3/4 fixes) that
will likely recur:

- **Every `@KafkaListener`/`@KafkaHandler` combo must have a handler for
  EVERY publishable event type on that topic** — even ones the listener
  has nothing to do in response to (a minimal log-and-return handler is
  fine, an absent one is not). An unhandled type doesn't get silently
  skipped, it throws `KafkaException: No method found` and wedges the
  partition for that consumer group until fixed. See "Post-Phase 4 fix"
  above for the full incident (this bit `StockReleased` in Phase 4, live,
  the first time compensation was ever actually exercised end-to-end).
  When shipping-service starts publishing `ShipmentCreated`/
  `ShipmentFailed`, double check `ShippingEventListener` already covers
  both (it does, as of Phase 2) — and if shipping-service or query-service
  end up listening to any OTHER multi-type topic later, apply this check
  there too before considering that listener done.

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

---

## Phase 9 — Observability + Dockerization

Planned in `docs/phase9-plan.md` (agreed with the user before
implementation) and executed in 5 parts, one at a time, with the user
reviewing after each part. This section is the permanent writeup —
`phase9-plan.md` can be deleted or kept as historical context.

### Part A — Micrometer + Prometheus on all 5 services
`implementation("io.micrometer:micrometer-registry-prometheus")` added to
each of the 5 `build.gradle.kts` files (no explicit version — resolved via
the Spring Boot 4.1.1 BOM already in play, same no-version style used for
every other starter in this project), plus
`management.endpoints.web.exposure.include=health,prometheus` in each
`application.properties`. All 5 services already had
`spring-boot-starter-actuator`; none had the Prometheus registry or any
`management.*` config before this phase.

This alone is enough for Spring Kafka's built-in Micrometer binder
(`MicrometerConsumerListener`/`MicrometerProducerListener`, transitively
present once `micrometer-core` is on the classpath) to start auto-
instrumenting Kafka producer/consumer client metrics — no extra code.

### Part B — Custom Saga metrics (order-service only)
Only `order-service` is the Saga orchestrator with visibility over an
order's entire lifecycle; the other 4 services are participants with no
cross-service view, so this part touches only `order-service`.

- **`Order.java`**: gained a `createdAt` field (populated from
  `OrderCreated.occurredAt()` in `apply()`) and a `getCreatedAt()` getter
  — needed because a live `Timer` measuring Saga duration needs the start
  timestamp at the moment the terminal transition happens; it can't be
  computed retroactively.
- **`OrderCommandService`**: `MeterRegistry` added as a 4th constructor
  parameter (alongside `eventStore`/`outboxWriter`/`commandFactory`,
  already constructor-injected).
- **`orbit.saga.duration`** (`Timer`): recorded via a new private
  `recordSagaDuration(Order, String outcome)` helper, called from the 4
  handlers that transition an order to a terminal state —
  `handleShipmentCreated` (`outcome=COMPLETED`), `handleStockRejected`
  (`CANCELLED`), `handlePaymentDeclined` (`CANCELLED`),
  `handleShipmentFailed` (`FAILED`). Duration is
  `Duration.between(order.getCreatedAt(), Instant.now())`.
- **`orbit.saga.compensation`** (`Counter`): recorded via a new private
  `recordCompensation(String trigger)` helper, called only from the 3
  compensation-triggering handlers, tagged `trigger` ∈
  `{STOCK_REJECTED, PAYMENT_DECLINED, SHIPMENT_FAILED}` — deliberately
  the *name of the handler that fired*, not the triggering event's
  free-text `reason` field. Prometheus time series are created per unique
  label value combination; a free-text field as a tag means one new time
  series per distinct string ever seen, unbounded and never cleaned up
  (a well-known Prometheus footgun). Both metrics' tags are fixed,
  small, enumerable sets instead.

### Part B addendum — `publishPercentileHistogram()` needed for the p50/p95 Grafana panels
Found while building the Grafana dashboard in Part E, added back into
Part B's code: a plain `Timer.builder(...).register(...)` only exports
`_count`/`_sum`/`_max` to Prometheus — not enough for
`histogram_quantile(...)`, which needs cumulative histogram buckets
(the `_bucket` series). `Timer.builder(SAGA_DURATION_METRIC).tag(...)
.publishPercentileHistogram().register(meterRegistry)` was needed for
`orbit_saga_duration_seconds_bucket` to actually exist, which is what
`saga-overview.json`'s p50/p95-by-outcome panels query via
`histogram_quantile(0.50, sum(rate(orbit_saga_duration_seconds_bucket
[5m])) by (le, outcome))`. The same gap applies to Spring Boot's own
built-in `http.server.requests` timer (used by the JVM/HTTP dashboard's
p95 latency panel) — see Part E below.

### Part C — Kafka Streams metrics: no code needed, Spring Boot 4 auto-configures it
`phase9-plan.md` planned a manual `StreamsBuilderFactoryBeanConfigurer`
bean in `UserActivityStreamsConfig`, registering
`KafkaStreamsMicrometerListener` on the `defaultKafkaStreamsBuilder`
`StreamsBuilderFactoryBean` (`@EnableKafkaStreams` already creates it) —
this is the standard approach in older Spring Boot / Spring Kafka
versions, where this wiring is NOT auto-configured, and was assumed to
still be true here based on general Micrometer-Kafka-Streams integration
knowledge.

Before writing that bean, the actual `spring-boot-kafka-4.0.6.jar`
resolved by this project's Spring Boot 4.1.1 BOM was inspected directly
(bytecode, via `javap`) rather than trusting that assumption, and it
turned out to be wrong for this specific version: Spring Boot 4 ships a
first-class auto-configuration,
`org.springframework.boot.kafka.autoconfigure.metrics
.KafkaMetricsAutoConfiguration.KafkaStreamsMetricsConfiguration`,
which registers exactly the same `StreamsBuilderFactoryBeanConfigurer`
bean the plan proposed writing by hand:
```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ KafkaStreamsMetrics.class, StreamsBuilderFactoryBean.class })
class KafkaStreamsMetricsConfiguration {
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    StreamsBuilderFactoryBeanConfigurer kafkaStreamsMetrics(MeterRegistry registry) {
        return factoryBean -> factoryBean.addListener(new KafkaStreamsMicrometerListener(registry));
    }
}
```
Both conditions (`KafkaStreamsMetrics`/`StreamsBuilderFactoryBean` on the
classpath, a `MeterRegistry` bean present) were already satisfied in
`query-service` the moment Part A landed — `kafka-streams` was already a
dependency (Phase 7), and `micrometer-registry-prometheus` (Part A)
registers the `MeterRegistry` bean. No property gate exists; this is
opt-out, not opt-in.

**Net result:** zero functional code change in `query-service`.
Registering the same listener manually would have double-bound metrics
against the same `KafkaStreams` instance. Instead, `UserActivityStreamsConfig`
gained a detailed Javadoc paragraph explaining this finding, so a future
reader doesn't waste time re-deriving it (or worse, re-adding the
now-redundant bean). Kafka Streams' own internal metrics (state store
metrics, per-task/thread metrics, rebalance counts) are exposed on
`/actuator/prometheus` automatically, the same way consumer/producer
metrics are (Part A) — no manual instrumentation needed, unlike the
`orbit.saga.*` metrics in Part B, which genuinely have no automatic
equivalent since they're project-specific business metrics.

**Lesson for future phases:** when a plan makes a claim about what a
framework does or doesn't auto-configure, especially across a major
version bump (Spring Boot 4 restructured its autoconfigure packages
significantly — see `org.springframework.boot.kafka.autoconfigure.*`
vs. the pre-4.0 `org.springframework.boot.autoconfigure.kafka.*`),
verify against the actual resolved jar before writing code, not just
general framework knowledge that may predate the version in use.

### Part D — Dockerizing the 5 Java services
Expanded scope, user-requested: simplifies Prometheus scrape config (real
service DNS names inside the Docker network instead of
`host.docker.internal`) and makes the whole observability stack
self-contained. No Dockerfile existed anywhere in the project before this
phase; all 5 services ran via `bootRun` against Dockerized infra only.

- **`.dockerignore`** (new, repo root) — excludes `.git/`, every module's
  `build/`/`.gradle/`/`.idea/`, `ingestion-service/` build artifacts,
  `docs/`, `README.md`, `TASKS.md`, etc.
- **One `Dockerfile` per service**, multi-stage, **build context = repo
  root** (`docker build -f order-service/Dockerfile .`) — required because
  this is a Gradle multi-module monorepo: each service depends on
  `:event-schemas`/`:outbox-support` at the root, so Docker needs to see
  the whole tree, not just the service's own folder.
  - Stage `build`: `eclipse-temurin:21-jdk-alpine`, `COPY . .` (the whole
    repo, filtered by `.dockerignore`), `./gradlew :service-name:bootJar
    --no-daemon` (`--no-daemon` avoids a lingering Gradle daemon process
    keeping the build-stage container alive after the task finishes).
  - Stage `runtime`: `eclipse-temurin:21-jre-alpine`, copies only
    `build/libs/*.jar` from the build stage as `app.jar`, `ENTRYPOINT
    ["java", "-jar", "app.jar"]` — none of the JDK, Gradle wrapper, source
    tree, or other modules' build output ends up in the runtime image.
- **`application.properties`** (all 5 services): every hardcoded
  `localhost` reference (`spring.datasource.url`,
  `spring.kafka.bootstrap-servers`, every `...schema.registry.url`
  occurrence including query-service's Streams properties) became an
  env-var placeholder with the current value as the default —
  `${DB_HOST:localhost}`, `${KAFKA_BOOTSTRAP:localhost:9092}`,
  `${SCHEMA_REGISTRY_URL:http://localhost:8081}`. Running via `bootRun`
  locally with no env vars set behaves identically to before this phase;
  only the Compose `apps` profile (below) actually sets these vars.
- **`docker-compose.yml`**: 5 new services (`order-service`,
  `inventory-service`, `payment-service`, `shipping-service`,
  `query-service`), each under `profiles: ["apps"]`, `build: { context:
  ., dockerfile: <service>/Dockerfile }`, `depends_on` on
  `postgres`/`broker`/`schema-registry` with `condition: service_healthy`
  (matching the existing pattern used by `kafka-init`/`schema-registry`/
  `kafka-ui`), `environment: { DB_HOST: postgres, KAFKA_BOOTSTRAP:
  broker:19092, SCHEMA_REGISTRY_URL: http://schema-registry:8081 }`, and
  the existing host port mappings preserved (`8082:8082`, etc.) so manual
  `curl`/Postman testing against `localhost:808X` keeps working whether a
  service is running via `bootRun` or via Compose. Container names follow
  the existing `orbit-<name>` convention.
- **Compose profile strategy**: all 5 services (and, per Part E, the
  observability stack too) live under a new `apps` profile, not the
  default one. Plain `docker compose up -d` (today's exact command)
  keeps bringing up ONLY the pre-existing infra (`broker`, `kafka-init`,
  `schema-registry`, `postgres`, `kafka-ui`) — zero change to the
  established `bootRun`-based dev workflow from every prior phase.
  `docker compose --profile apps up -d --build` brings up everything.
- **`scripts/env-up.ps1`**: gained an `-Apps` switch that prepends
  `--profile apps` to the `docker compose up` invocation and prints the
  5 services' URLs (plus Prometheus/Grafana, added in Part E) when used.
- **`ingestion-service` (Go) stays undockerized** this phase — not part
  of the original observability request, doesn't participate in the
  Saga, can be revisited later if ever needed.

Validated via `docker compose config --quiet` (both with and without
`COMPOSE_PROFILES=apps` set) and `.\gradlew.bat compileJava
compileTestJava` after the `application.properties` changes. The actual
`docker compose --profile apps up -d --build` + manual Grafana/Prometheus
verification was deferred to the user's personal machine — the corporate
machine used for implementation hits a Docker build-time certificate
error unrelated to this project's configuration.

### Part E — Observability infra (Prometheus, Grafana, kafka-exporter)
- **`infra/prometheus/prometheus.yml`**: scrape configs targeting
  Docker-network service DNS names (`order-service:8082`,
  `inventory-service:8083`, `payment-service:8084`,
  `shipping-service:8085`, `query-service:8086`, all at
  `/actuator/prometheus`), plus `kafka-exporter:9308` and Prometheus's
  own self-scrape.
- **`docker-compose.yml`** additions (all under the `apps` profile — a
  Prometheus instance with no Java services running has nothing useful
  to scrape, so it's bundled with the apps rather than the default
  profile):
  - `prometheus` (`prom/prometheus:v3.14.0`), container
    `orbit-prometheus`, port `9090:9090`, mounts
    `./infra/prometheus/prometheus.yml:ro`, `depends_on` the 5 app
    services (`condition: service_started` — no healthcheck needed for
    pure scrape targets).
  - `kafka-exporter` (`danielqsj/kafka-exporter:v1.9.0`), container
    `orbit-kafka-exporter`, `--kafka.server=broker:19092` (the internal
    listener, same one `kafka-init`/`schema-registry` already use, not
    the external `PLAINTEXT_HOST` one), `depends_on: broker
    (service_healthy)`.
  - `grafana` (`grafana/grafana:13.2.1`), container `orbit-grafana`, port
    `3000:3000`, mounts
    `./infra/grafana/provisioning:/etc/grafana/provisioning:ro`,
    `depends_on: prometheus`.
- **Image version pins**: all 3 explicitly versioned (`v3.14.0`,
  `13.2.1`, `v1.9.0`), consistent with almost everything else in the
  project (`postgres:16-alpine`, `apache/kafka:4.3.1`, etc.) — verified
  as the latest stable/explicitly-versioned tags via Docker Hub before
  picking them. `kafka-ui`'s `:latest` remains the documented exception
  (Phase 1), not a pattern extended further.
- **`infra/grafana/provisioning/datasources/prometheus.yml`**: Grafana
  provisioning-as-code datasource pointing at `http://prometheus:9090`,
  auto-loaded on startup, no manual UI setup needed.
- **`infra/grafana/provisioning/dashboards/dashboards.yml`**: dashboard
  provider config, pointing Grafana at the same folder for JSON
  dashboard files to auto-load.
- **Three dashboards**, per the hybrid decision made with the user
  (hand-build only the Saga-specific one from scratch, adapt the generic
  JVM/HTTP and Kafka-consumer-lag ones from well-known community
  dashboard shapes rather than importing them verbatim, since Grafana's
  file-based provisioning here needs self-contained JSON, not an
  internet-fetched import):
  - **`saga-overview.json`** — `orbit.saga.duration` p50/p95 by
    `outcome` (via `histogram_quantile(...)` over
    `orbit_saga_duration_seconds_bucket` — see the Part B addendum
    above for why `publishPercentileHistogram()` was required),
    `orbit.saga.compensation` rate by `trigger`, Saga completions by
    outcome, and `POST /orders` throughput.
  - **`jvm-overview.json`** — heap used, live threads, GC pause rate,
    process CPU usage, HTTP request rate and p95 latency, all templated
    by a `$job` variable (multi-select across all 5 services' Prometheus
    job labels) so one dashboard covers every service instead of
    needing 5 near-identical copies.
  - **`kafka-consumer-lag.json`** — consumer group lag by
    topic/partition, lag summed by consumer group, topic partition
    offsets, and broker/topic message rate — all from kafka-exporter's
    `kafka_consumergroup_lag`/`kafka_topic_partition_current_offset`
    metrics.
- **HTTP request histogram**: `management.metrics.distribution
  .percentiles-histogram.http.server.requests=true` added to all 5
  `application.properties` (found necessary while building
  `jvm-overview.json`'s p95 latency panel — same underlying reason as
  the Part B addendum: Spring Boot's built-in `http.server.requests`
  `Timer` doesn't export histogram buckets by default either, so
  `histogram_quantile(...)` would have nothing to compute against
  without this).
- **`scripts/env-up.ps1`**: `-Apps` banner extended with `Prometheus:
  http://localhost:9090` and `Grafana: http://localhost:3000 (admin/admin
  default login)`.

Validated: `docker compose config --quiet` (default and `apps` profile),
all 3 dashboard JSON files parsed successfully via PowerShell's
`ConvertFrom-Json`, the 3 new/changed YAML provisioning files reviewed
manually for indentation correctness, and `.\gradlew.bat compileJava
compileTestJava` succeeded after the `Timer.publishPercentileHistogram()`
and `application.properties` changes. As with Part D, the actual
`docker compose --profile apps up -d --build` run plus manual
Prometheus-targets/Grafana-dashboard verification is deferred to the
user's personal machine.

### Ports used by this phase
`9090` (Prometheus), `3000` (Grafana), `9308` (kafka-exporter) — all
confirmed free beforehand (no conflicts with the existing
`8080`-`8086`, `9092`, `9093`, `19092`, `5432`).
