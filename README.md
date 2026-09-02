# orbit-commerce

Event-driven e-commerce platform built as a showcase of Java 21+, Spring Boot 4,
Apache Kafka, Docker and microservices architecture.

The order fulfillment flow is implemented as an **orchestrated Saga** across five
Spring Boot services, backed by an **Outbox pattern** for transactional event
publishing, **Event Sourcing + CQRS** for the order aggregate and its read model,
and **Kafka Streams** for real-time analytics. A Go service produces high-volume
user activity events.

## Services

| Service | Language | Responsibility |
|---|---|---|
| `order-service` | Java 21 / Spring Boot 4 | Saga orchestrator, event-sourced `Order` aggregate |
| `payment-service` | Java 21 / Spring Boot 4 | Payment processing / refunds |
| `inventory-service` | Java 21 / Spring Boot 4 | Stock reservation / release |
| `shipping-service` | Java 21 / Spring Boot 4 | Shipment creation |
| `query-service` | Java 21 / Spring Boot 4 | CQRS read model + Kafka Streams analytics |
| `ingestion-service` | Go | High-throughput user activity event producer |
| `event-schemas` | Java (Avro) | Shared Avro schemas for all Kafka topics |

See [TASKS.md](./TASKS.md) for the full implementation plan and progress.

## Status

Phases 0 and 1 are complete: module scaffolding, build tooling, shared Avro
schemas, and the local Docker Compose infrastructure (Kafka, Schema Registry,
Postgres, Kafka UI) are all in place and validated. Business logic
implementation starts in Phase 2.

## Development

Requires: JDK 21, Docker Desktop (with Compose v2), Go 1.22+.

### Local infrastructure

```powershell
# Bring up Kafka (KRaft), Schema Registry, Postgres, and Kafka UI
.\scripts\env-up.ps1

# Tail logs (optionally for a single service, e.g. -Service broker)
.\scripts\env-logs.ps1

# Tear down (add -Volumes to also wipe data)
.\scripts\env-down.ps1
```

Once up, the following are available:

| Service | URL / Address |
|---|---|
| Kafka broker | `localhost:9092` |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Postgres | `localhost:5432` (user/password: `orbit` / `orbit`) |

Kafka topics (`order.events`, `inventory.commands`, `inventory.events`,
`payment.commands`, `payment.events`, `shipping.commands`, `shipping.events`,
`user-activity.events`) and the five per-service Postgres databases
(`order_db`, `payment_db`, `inventory_db`, `shipping_db`, `query_db`) are
created automatically on first startup.

### Building the code

```powershell
# Build all Java/Gradle modules
.\gradlew.bat build

# Build the Go ingestion-service
cd ingestion-service
go build ./...
```
