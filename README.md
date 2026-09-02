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

Project scaffolding (Phase 0) — module structure, build tooling, and shared
Avro schemas are in place. Business logic implementation starts in Phase 1.

## Development

Requires: JDK 21, Docker.

```powershell
# Build all Java/Gradle modules
.\gradlew.bat build

# Build the Go ingestion-service
cd ingestion-service
go build ./...
```
