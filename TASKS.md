# orbit-commerce — Task Breakdown

Legenda de prioridade: P0 (crítico/bloqueante) · P1 (essencial) · P2 (nice-to-have)

## Fase 0 — Scaffolding do repositório
- [ ] P0 Criar estrutura de monorepo (Gradle multi-módulo + módulo Go separado)
- [ ] P0 `settings.gradle.kts` com módulos: order-service, payment-service, inventory-service,
      shipping-service, query-service, event-schemas
- [ ] P0 Módulo `event-schemas`: schemas Avro (.avsc) para eventos/comandos de order, inventory,
      payment, shipping, user-activity
- [ ] P0 Configurar plugin Avro no Gradle para geração de classes Java
- [ ] P1 `go.mod` para ingestion-service, estrutura de pastas Go (cmd/, internal/)
- [ ] P1 `.editorconfig`, `.gitignore` (Java + Go + Docker)
- [ ] P2 README inicial com visão geral da arquitetura (placeholder)

## Fase 1 — Infraestrutura base (Docker Compose)
- [ ] P0 `docker-compose.yml`: Kafka em modo KRaft (sem Zookeeper)
- [ ] P0 Confluent Schema Registry
- [ ] P0 Postgres único + init-script criando DBs: order_db, payment_db, inventory_db,
      shipping_db, query_db
- [ ] P1 Kafka UI (Provectus) para inspeção de tópicos/schemas
- [ ] P1 Criação dos tópicos Kafka via script de init (order.events, inventory.commands,
      inventory.events, payment.commands, payment.events, shipping.commands, shipping.events,
      user-activity.events)
- [ ] P2 Prometheus + Grafana + kafka-exporter (pode ser adiado para Fase 8)
- [ ] P1 `Makefile` ou scripts PowerShell para subir/derrubar o ambiente local

## Fase 2 — order-service (núcleo da Saga)
- [ ] P0 Setup Spring Boot 4 (Java 21, virtual threads habilitadas)
- [ ] P0 Modelo de domínio `Order` como aggregate event-sourced (estados: CREATED,
      STOCK_RESERVED, PAYMENT_APPROVED, SHIPPED, COMPLETED, CANCELLED, FAILED)
- [ ] P0 Event store (tabela `order_events`) + tabela `outbox`
- [ ] P0 API REST: `POST /orders`, `GET /orders/{id}`
- [ ] P0 Publicação de `OrderCreated` via outbox + poller (@Scheduled)
- [ ] P0 Orquestração da Saga: envio de comandos (ReserveStockCommand,
      ProcessPaymentCommand, CreateShipmentCommand) reagindo a eventos recebidos
- [ ] P0 Listeners para StockReserved/StockRejected, PaymentApproved/PaymentDeclined,
      ShipmentCreated/ShipmentFailed
- [ ] P0 Lógica de compensação (cancelamento, reembolso, liberação de stock)
- [ ] P1 Idempotência no consumo de eventos (deduplicação por eventId)
- [ ] P1 Correlation ID (orderId) propagado em headers Kafka
- [ ] P1 Testes de integração com Testcontainers (Kafka + Postgres) — happy path e
      pelo menos 1 caminho de compensação

## Fase 3 — inventory-service
- [ ] P0 Setup Spring Boot 4 + Postgres (inventory_db)
- [ ] P0 Modelo de stock por produto + endpoint de seed inicial
- [ ] P0 Consumer de `ReserveStockCommand` → grava outbox com StockReserved/StockRejected
- [ ] P0 Consumer de `ReleaseStockCommand` (compensação) → StockReleased
- [ ] P1 Idempotência de consumo
- [ ] P1 Testes de integração (Testcontainers)

## Fase 4 — payment-service
- [ ] P0 Setup Spring Boot 4 + Postgres (payment_db)
- [ ] P0 Consumer de `ProcessPaymentCommand` → simula aprovação/recusa (regra simples,
      ex: valor > limite = recusa) → PaymentApproved/PaymentDeclined via outbox
- [ ] P0 Consumer de `RefundPaymentCommand` (compensação) → PaymentRefunded
- [ ] P1 Idempotência de consumo
- [ ] P1 Testes de integração (Testcontainers)

## Fase 5 — shipping-service
- [ ] P0 Setup Spring Boot 4 + Postgres (shipping_db)
- [ ] P0 Consumer de `CreateShipmentCommand` → simula criação de envio (pode falhar
      propositalmente para testar compensação) → ShipmentCreated/ShipmentFailed via outbox
- [ ] P1 Idempotência de consumo
- [ ] P1 Testes de integração (Testcontainers)

## Fase 6 — Saga end-to-end
- [ ] P0 Validar fluxo completo happy path (order → stock → payment → shipping → completed)
- [ ] P0 Validar compensação: stock rejeitado → order cancelado
- [ ] P0 Validar compensação: pagamento recusado → stock liberado → order cancelado
- [ ] P0 Validar compensação: falha no envio → reembolso + liberação de stock → order failed
- [ ] P1 Documentar diagramas de sequência (Mermaid) para os 4 cenários acima

## Fase 7 — query-service (CQRS + Kafka Streams)
- [ ] P0 Setup Spring Boot 4 + Postgres (query_db, read-model)
- [ ] P0 Consumers de order.events/inventory.events/payment.events/shipping.events →
      materializar timeline do pedido (read-model)
- [ ] P0 API REST: `GET /orders/{id}/timeline`, `GET /orders?status=`
- [ ] P1 Kafka Streams topology sobre `user-activity.events`: tumbling window (1 min) de
      ProductViewed agrupado por productId, contagem via state store
- [ ] P1 Endpoint de interactive query: `GET /analytics/top-products`
- [ ] P2 Testes de integração (Testcontainers, incl. Kafka Streams TopologyTestDriver)

## Fase 8 — ingestion-service (Go)
- [ ] P0 Setup projeto Go (kafka-go ou confluent-kafka-go)
- [ ] P0 Gerador de eventos de atividade (ProductViewed, AddedToCart, SearchPerformed)
      com worker pool/goroutines para simular alto volume
- [ ] P0 Serialização Avro compatível com Schema Registry, publicação em
      `user-activity.events`
- [ ] P1 Endpoint HTTP simples (ex: `/health`, `/stats`) usando net/http ou chi
- [ ] P1 Configuração via env vars (taxa de eventos, número de workers)
- [ ] P2 Testes unitários Go (table-driven tests)

## Fase 9 — Observabilidade
- [ ] P1 Micrometer + Prometheus em todos os serviços Spring
- [ ] P1 Métricas customizadas: duração da saga, taxa de compensação por tipo de falha
- [ ] P1 kafka-exporter para métricas de consumer lag
- [ ] P1 Dashboards Grafana (throughput, latência da saga, lag de consumers)
- [ ] P2 Tracing distribuído (OpenTelemetry + Jaeger/Tempo) — se houver tempo

## Fase 10 — Documentação e polimento
- [ ] P0 README principal: visão geral, arquitetura, como subir o ambiente
- [ ] P0 Diagrama de arquitetura geral (Mermaid ou imagem)
- [ ] P1 Diagrama de sequência da Saga (todos os cenários)
- [ ] P1 Catálogo de eventos documentado (tabela: tópico, schema, produtor, consumidores)
- [ ] P2 Guia de troubleshooting / decisões de arquitetura (ADRs curtos)
- [ ] P2 GitHub Actions: build + testes em CI para todos os módulos (Java e Go)
