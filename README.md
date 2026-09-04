# Event-Driven Order & Payment Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/FastAPI-0.110+-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Apache_Kafka-7.5-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/Redis-7.0-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Tests-57%20Passed-success?style=for-the-badge&logo=pytest&logoColor=white" alt="Tests" />
  <img src="https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge" alt="License" />
</p>

---

## Executive Summary

**Goal:** Build a realistic, production-grade e-commerce backend where orders, payments, inventory, and notifications communicate seamlessly through Kafka events, backed by ACID transactions, the Transactional Outbox pattern, consumer idempotency, dead-letter queues, and high-concurrency safety.

### Core Stack
* **Language & API:** Python 3.11+ / 3.14 + FastAPI
* **Primary Database:** PostgreSQL 15 (Independent databases per microservice)
* **Message Broker:** Apache Kafka (Event bus)
* **Distributed Cache & Rate Limiting:** Redis 7.0
* **Containerization:** Docker & Docker Compose
* **ORM & Migrations:** SQLAlchemy (AsyncIO) + Alembic
* **Data Validation:** Pydantic v2
* **Automated Testing:** pytest + pytest-asyncio (Unit, Integration, Concurrency, Failure & Chaos)
* **Observability:** Prometheus metrics (`/metrics`) + Structured JSON Logging + Health Probes (`/health/live`, `/health/ready`) + OpenTelemetry ready

---

## Table of Contents
1. [High-Level Project Diagram](#1-high-level-project-diagram)
2. [The Actual Business Flow](#2-the-actual-business-flow)
3. [Failure Flow & Compensation](#3-failure-flow)
4. [Project Structure (Monorepo)](#4-project-structure)
5. [Microservices Scope & Architecture](#5-dont-make-10-microservices)
6. [Kafka Topics & Event Catalogue](#6-kafka-topics)
7. [Database Design & Schema](#7-database-design)
8. [Critical Feature: Idempotency](#8-critical-feature-idempotency)
9. [Critical Feature: Transactional Outbox](#9-critical-feature-transactional-outbox)
10. [Critical Feature: Retries & Dead Letter Queue (DLQ)](#10-retry--dlq)
11. [Order State Machine](#11-order-state-machine)
12. [What Makes This Project Production-Level?](#12-what-makes-this-project-production-level)
13. [Recommended Implementation Order & Status](#recommended-implementation-order)
14. [Quickstart & Running Tests](#quickstart--running-tests)

---

## 1. High-Level Project Diagram

```text
                         ┌─────────────────────┐
                         │       CLIENT        │
                         │  Web / Mobile / API │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     API GATEWAY     │
                         │ Auth / Rate Limit   │
                         │ Request Validation  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────┐
                    │        ORDER SERVICE        │
                    │                             │
                    │ Create Order                │
                    │ Get Order                   │
                    │ Cancel Order                │
                    │ Order State Machine         │
                    └──────────────┬──────────────┘
                                   │
                     ┌─────────────┴─────────────┐
                     │                           │
                     ▼                           ▼
              ┌─────────────┐             ┌──────────────┐
              │ PostgreSQL  │             │    Redis     │
              │ Order DB    │             │ Cache / Lock │
              └─────────────┘             └──────────────┘
                     │
                     │ Transactional Outbox
                     ▼
              ┌─────────────────┐
              │  OUTBOX TABLE   │
              └────────┬────────┘
                       │
                       ▼
                 ┌───────────┐
                 │   KAFKA   │
                 │ Event Bus │
                 └─────┬─────┘
                       │
          ┌────────────┼─────────────┐
          │            │             │
          ▼            ▼             ▼
 ┌──────────────┐ ┌─────────────┐ ┌───────────────┐
 │   PAYMENT    │ │  INVENTORY  │ │ NOTIFICATION  │
 │   SERVICE    │ │   SERVICE   │ │    SERVICE    │
 └──────┬───────┘ └──────┬──────┘ └───────────────┘
        │                 │
        ▼                 ▼
 ┌──────────────┐  ┌──────────────┐
 │ PostgreSQL   │  │ PostgreSQL   │
 │ Payment DB   │  │ Inventory DB │
 └──────────────┘  └──────────────┘
        │                 │
        ▼                 ▼
 PaymentSucceeded    InventoryReserved
 PaymentFailed       InventoryFailed
        │                 │
        └────────┬────────┘
                 ▼
              ┌───────┐
              │ Kafka │
              └───┬───┘
                  │
                  ▼
            ┌─────────────┐
            │    ORDER    │
            │   SERVICE   │
            └──────┬──────┘
                   │
                   ▼
             Update Order
                Status
```

---

## 2. The Actual Business Flow

This is the core operational flow of the distributed system.

### Client Request
A customer initiates an order:
```http
POST /orders/
Idempotency-Key: 7f8a9b1c-2d3e-4f5a-6b7c-8d9e0f1a2b3c
Content-Type: application/json
```

```json
{
  "customer_id": "c1010000-0000-0000-0000-000000000101",
  "items": [
    {
      "product_id": "p5010000-0000-0000-0000-000000000501",
      "quantity": 2
    }
  ]
}
```

### Execution Lifecycle
```text
Client
  │
  ▼
Order Service
  │
  ├── Validate request
  ├── Check product
  ├── Calculate price
  ├── Create order (ACID Transaction)
  └── Create Outbox Event (OrderCreated)
          │
          ▼
       Kafka (orders.events)
          │
          ▼
    OrderCreated
       │
       ├──────────────────────────────┐
       ▼                              ▼
  Payment Service              Inventory Service
       │                              │
       ▼                              ▼
  Payment Processing             Reserve Stock
       │                              │
       ▼                              ▼
 PaymentSucceeded              InventoryReserved
       │                              │
       └──────────────┬───────────────┘
                      ▼
             Kafka (Reply Topics)
                      │
                      ▼
                Order Service
                      │ (Row-Locked State Machine)
                      ▼
                  CONFIRMED
```

---

## 3. Failure Flow

The true test of a distributed architecture is how it reacts when things go wrong.

### Scenario: Payment Failure
Suppose the payment provider fails or the card is declined:

```text
OrderCreated
     │
     ▼
 Payment Service
     │
     ▼
 Payment Failed (Card Declined / Gateway Error)
     │
     ▼
 PaymentFailed event
     │
     ▼
    Kafka (payments.events)
     │
     ▼
 Order Service
     │
     ▼
ORDER = PAYMENT_FAILED
```

### Compensation: Inventory Release
When payment fails, previously reserved stock must be released back to the pool to prevent stock leakage:

```text
PaymentFailed
      │
      ▼
Inventory Service
      │
      ▼
Release Reservation (available_quantity += quantity)
```

---

## 4. Project Structure

Organized as an enterprise clean-architecture monorepo with strict boundary isolation:

```text
event-driven-order-platform/
├── backend/
│   ├── services/
│   │   ├── order-service/
│   │   │   ├── app/
│   │   │   │   ├── api/
│   │   │   │   │   ├── routes/
│   │   │   │   │   │   ├── orders.py
│   │   │   │   │   │   └── health.py
│   │   │   │   │   └── dependencies.py
│   │   │   │   ├── core/
│   │   │   │   │   ├── config.py
│   │   │   │   │   ├── logging.py
│   │   │   │   │   ├── redis.py
│   │   │   │   │   └── security.py
│   │   │   │   ├── models/
│   │   │   │   │   ├── order.py
│   │   │   │   │   ├── order_item.py
│   │   │   │   │   ├── outbox.py
│   │   │   │   │   └── processed_event.py
│   │   │   │   ├── schemas/
│   │   │   │   │   ├── order.py
│   │   │   │   │   └── events.py
│   │   │   │   ├── services/
│   │   │   │   │   ├── order_service.py
│   │   │   │   │   └── pricing_service.py
│   │   │   │   ├── repositories/
│   │   │   │   │   ├── order_repository.py
│   │   │   │   │   └── outbox_repository.py
│   │   │   │   ├── messaging/
│   │   │   │   │   ├── producer.py
│   │   │   │   │   ├── consumer.py
│   │   │   │   │   └── outbox_publisher.py
│   │   │   │   ├── db/
│   │   │   │   │   ├── session.py
│   │   │   │   │   └── base.py
│   │   │   │   └── main.py
│   │   │   ├── tests/
│   │   │   │   ├── unit/
│   │   │   │   ├── integration/
│   │   │   │   └── api/
│   │   │   ├── alembic/
│   │   │   ├── Dockerfile
│   │   │   └── pyproject.toml
│   │   │
│   │   ├── payment-service/
│   │   │   ├── app/
│   │   │   │   ├── api/
│   │   │   │   ├── core/
│   │   │   │   ├── models/
│   │   │   │   ├── schemas/
│   │   │   │   ├── services/
│   │   │   │   ├── repositories/
│   │   │   │   ├── messaging/
│   │   │   │   ├── db/
│   │   │   │   └── main.py
│   │   │   ├── tests/
│   │   │   ├── alembic/
│   │   │   ├── Dockerfile
│   │   │   └── pyproject.toml
│   │   │
│   │   ├── inventory-service/
│   │   │   ├── app/
│   │   │   │   ├── api/
│   │   │   │   ├── core/
│   │   │   │   ├── models/
│   │   │   │   ├── schemas/
│   │   │   │   ├── services/
│   │   │   │   ├── repositories/
│   │   │   │   ├── messaging/
│   │   │   │   ├── db/
│   │   │   │   └── main.py
│   │   │   ├── tests/
│   │   │   ├── alembic/
│   │   │   ├── Dockerfile
│   │   │   └── pyproject.toml
│   │   │
│   │   └── notification-service/
│   │       ├── app/
│   │       │   ├── core/
│   │       │   ├── messaging/
│   │       │   ├── services/
│   │       │   └── main.py
│   │       ├── tests/
│   │       ├── Dockerfile
│   │       └── pyproject.toml
│   │
│   ├── shared/
│   │   ├── shared/
│   │   │   ├── events/
│   │   │   │   ├── event_types.py
│   │   │   │   ├── schemas.py
│   │   │   │   └── consumer_handler.py
│   │   │   ├── observability/
│   │   │   │   ├── logging.py
│   │   │   │   └── metrics.py
│   │   │   ├── redis/
│   │   │   │   ├── client.py
│   │   │   │   └── rate_limiter.py
│   │   │   └── exceptions/
│   │   │       └── errors.py
│   │   ├── tests/
│   │   └── pyproject.toml
│   │
│   └── infrastructure/
│       ├── postgres/
│       │   └── init.sql
│       └── prometheus/
│           └── prometheus.yml
│
├── .env.example
├── .gitignore
├── Makefile
├── docker-compose.yml
├── docker-compose.dev.yml
├── README.md
├── ANTIGRAVITY.md
└── LICENSE
```

---

## 5. Don't Make 10 Microservices

A common anti-pattern is premature decomposition into dozens of microservices:

* ❌ Customer Service
* ❌ Product Service
* ❌ Cart Service
* ❌ Pricing Service
* ❌ Shipping Service
* ❌ Analytics Service

That is unnecessary scope creep. For a resilient production demonstration, four focused services provide the ideal balance:

1. **Order Service:** Aggregates order lifecycle, initiates transactions, drives the state machine.
2. **Payment Service:** Interfaces with payment gateways, enforces billing idempotency.
3. **Inventory Service:** Manages stock levels with pessimistic & optimistic lock guarantees.
4. **Notification Service:** Listens to terminal events to dispatch user receipts/alerts.

---

## 6. Kafka Topics

A clean, predictable event catalogue partitionable by entity key:

| Topic | Purpose | Partitions |
| :--- | :--- | :--- |
| `orders.events` | Order lifecycle events (`OrderCreated`, `OrderCancelled`) | 3 |
| `payments.events` | Payment outcomes (`PaymentRequested`, `PaymentSucceeded`, `PaymentFailed`) | 3 |
| `inventory.events` | Stock reservations (`InventoryReserved`, `InventoryFailed`, `InventoryReleased`) | 3 |
| `notifications.events` | Customer alerts | 3 |
| `*.events.dlq` | Dedicated Dead Letter Queues for poisoned or failed messages | 3 |

### Event Catalogue
* **Order Domain:** `OrderCreated`, `OrderCancelled`
* **Payment Domain:** `PaymentRequested`, `PaymentSucceeded`, `PaymentFailed`, `RefundRequested`, `RefundSucceeded`
* **Inventory Domain:** `InventoryReservationRequested`, `InventoryReserved`, `InventoryReservationFailed`, `InventoryReleased`

### Standard Event Envelope
All events across the platform implement this immutable, versioned schema:

```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_type": "OrderCreated",
  "aggregate_type": "order",
  "aggregate_id": "7f8a9b1c-2d3e-4f5a-6b7c-8d9e0f1a2b3c",
  "occurred_at": "2026-09-02T10:30:00Z",
  "correlation_id": "c0a8012e-8412-4c28-98e3-4f9e1e5b2210",
  "causation_id": null,
  "version": 1,
  "payload": {
    "customer_id": "c1010000-0000-0000-0000-000000000101",
    "total_amount": "100.00",
    "currency": "USD",
    "items": [
      {
        "product_id": "p5010000-0000-0000-0000-000000000501",
        "quantity": 2,
        "unit_price": "50.00"
      }
    ]
  }
}
```

> **Interview Topic:** Why do we need `event_id`, `correlation_id`, and `version`?
> * `event_id`: Guarantees consumer deduplication and at-most-once processing.
> * `correlation_id`: Links all downstream actions across multiple microservices back to the original client request for distributed tracing and forensic debugging.
> * `version`: Facilitates schema evolution without breaking existing consumers.

---

## 7. Database Design

### Order Database (`orders_db`)
```text
orders
----------------
id (UUID, PK)
customer_id (UUID)
status (VARCHAR)
payment_status (VARCHAR)
inventory_status (VARCHAR)
total_amount (DECIMAL)
currency (VARCHAR)
idempotency_key (VARCHAR, Unique Index)
created_at (TIMESTAMP)
updated_at (TIMESTAMP)
version (INTEGER)

order_items
----------------
id (UUID, PK)
order_id (UUID, FK -> orders.id)
product_id (UUID)
quantity (INTEGER)
unit_price (DECIMAL)

outbox_events
----------------
id (UUID, PK)
event_id (UUID, Unique)
aggregate_id (UUID)
aggregate_type (VARCHAR)
event_type (VARCHAR)
correlation_id (UUID)
causation_id (UUID)
payload (JSONB)
status (VARCHAR: PENDING | PUBLISHED | FAILED)
created_at (TIMESTAMP)
published_at (TIMESTAMP)
retry_count (INTEGER)
last_error (TEXT)

processed_events
----------------
event_id (UUID, PK)
event_type (VARCHAR)
consumer_name (VARCHAR)
processed_at (TIMESTAMP)
```

### Payment Database (`payments_db`)
```text
payments
----------------
id (UUID, PK)
order_id (UUID)
idempotency_key (VARCHAR, Unique Index)
amount (DECIMAL)
currency (VARCHAR)
status (VARCHAR)
provider_reference (VARCHAR)
created_at (TIMESTAMP)
updated_at (TIMESTAMP)
```

### Inventory Database (`inventory_db`)
```text
inventory
----------------
product_id (UUID, PK)
available_quantity (INTEGER, CHECK >= 0)
reserved_quantity (INTEGER, CHECK >= 0)
version (INTEGER, Optimistic Locking)
updated_at (TIMESTAMP)

inventory_reservations
----------------
id (UUID, PK)
order_id (UUID)
product_id (UUID)
quantity (INTEGER)
status (VARCHAR: RESERVED | RELEASED)
created_at (TIMESTAMP)
```

---

## 8. Critical Feature: Idempotency

Kafka provides **at-least-once delivery**. If network partitions or rebalances occur, duplicate events **will** arrive.

```text
Without Idempotency:
PaymentSucceeded ──► Update order ──► Update order again ──► Send receipt again 💥

With Consumer Idempotency:
PaymentSucceeded ──► Check processed_events ──► Already processed ──► Safe NO-OP ✅
```

### Two-Tier Idempotency Architecture
1. **API Level (`Idempotency-Key` Header):**
   * Prevents double-charging if a client clicks "Pay" twice or retries after a network timeout.
   * Competing requests with the same key safely resolve via database unique constraints without throwing 500 errors.
2. **Consumer Level (`processed_events` Table):**
   * Before executing business logic, the consumer checks `processed_events`.
   * The business modification and the `processed_events` insert are committed in the **exact same ACID database transaction**.
   * If the transaction fails, neither is saved. If it succeeds, redelivered events are safely skipped.

---

## 9. Critical Feature: Transactional Outbox

A common mistake in microservices is updating the database and publishing directly to Kafka:

```text
❌ Anti-Pattern (Dual-Write Problem):
DB Transaction Commit ──► (Server Crashes / Network Fails) ──► Kafka Publish Never Runs!
Result: Database updated, but event lost forever!
```

### The Solution: Transactional Outbox Pattern
Both the domain entity and the outbox event are persisted atomically in the same database transaction:

```sql
BEGIN TRANSACTION;
  INSERT INTO orders (id, customer_id, total_amount, status) VALUES (...);
  INSERT INTO outbox_events (id, aggregate_id, event_type, payload, status) VALUES (...);
COMMIT;
```

```text
PostgreSQL
┌───────────────────────┐
│ orders                │
│ outbox_events         │
└───────────┬───────────┘
            │
            ▼
     Outbox Publisher (SELECT ... FOR UPDATE SKIP LOCKED)
            │
            ▼
          Kafka Bus
            │
            ▼
     Mark Outbox Row as PUBLISHED
```

> **High-Concurrency Guarantee:** The background publisher uses `SELECT ... FOR UPDATE SKIP LOCKED` so multiple worker instances can poll the queue concurrently without lock contention or duplicate messages.

---

## 10. Retry + DLQ

When downstream dependencies (like third-party payment gateways) experience transient outages, messages must not immediately fail permanently:

```text
Attempt 1 ──► FAIL ──► Backoff (0.5s) ──► Attempt 2 ──► FAIL ──► Backoff (1.0s) ──► Attempt 3 ──► FAIL ──► Route to DLQ
```

```text
Kafka (Input Topic)
  │
  ▼
Consumer Handler
  │
  ├── Transient Failure ──► Exponential Backoff Retry (Attempts 1..3)
  │
  └── Permanent Failure (Retries Exhausted)
         │
         ▼
     DeadLetterEnvelope (Error Type, Stack Trace, Original Offset)
         │
         ▼
     Kafka (*.events.dlq)
         │
         ▼
     Commit Offset (Unblocks Consumer Partition!)
```

> **Key Distributed Systems Principle:** Routing poison pills to DLQs and committing their offset prevents a single bad message from permanently stalling an entire partition.

---

## 11. Order State Machine

Arbitrary status transitions are strictly forbidden. Transitions are governed by an immutable state machine:

```text
                 ┌─────────────┐
                 │   CREATED   │
                 └──────┬──────┘
                        │
                        ▼
                 PAYMENT_PENDING
                   /          \
                  /            \
                 ▼              ▼
          PAYMENT_FAILED    PAYMENT_SUCCESS
                │                │
                ▼                ▼
             FAILED       INVENTORY_PENDING
                                  │
                           ┌──────┴──────┐
                           ▼             ▼
                     INVENTORY_FAILED  RESERVED
                           │             │
                           ▼             ▼
                         FAILED      CONFIRMED
                                         │
                                         ▼
                                      SHIPPED
                                         │
                                         ▼
                                      DELIVERED
```

* Invalid transitions (e.g. `DELIVERED → CREATED` or `CONFIRMED → CREATED`) raise explicit domain validation errors.
* Cross-topic ordering race conditions are defended using row-level pessimistic locks (`SELECT ... FOR UPDATE`).

---

## 12. What Makes This Project Production-Level?

Do not judge an architecture by the sheer number of microservices. Judge it by how reliably it handles real-world distributed systems challenges:

* [x] **ACID Transactions:** Clean transaction boundaries per service.
* [x] **Idempotency:** API keys and consumer deduplication tables.
* [x] **Event Ordering:** Entity-level partition hashing.
* [x] **Duplicate Events:** Handled gracefully via `processed_events`.
* [x] **Exponential Retries:** Built-in backoff for transient faults.
* [x] **Dead-Letter Queue:** Forensic error tracing with partition unblocking.
* [x] **Transactional Outbox:** Multi-worker `SKIP LOCKED` polling.
* [x] **Concurrency & Race Defenses:** Pessimistic row locking & optimistic versioning.
* [x] **Database Consistency:** PostgreSQL check constraints (`available_quantity >= 0`).
* [x] **Failure Recovery:** Crash-recovery tested with at-least-once redelivery.
* [x] **Correlation IDs:** Distributed tracing across HTTP and Kafka envelopes.
* [x] **Structured Logging:** Standard JSON formatted logs.
* [x] **Health Checks:** `/health/live` and `/health/ready` (DB + Redis checks).
* [x] **API Validation:** Strict boundary validation via Pydantic.
* [x] **Authentication & Authorization:** Header and identity dependencies.
* [x] **Rate Limiting:** Distributed Redis sliding-window limiter with fail-open safety.
* [x] **Automated Tests:** 57 automated unit, integration, and chaos tests (100% green).
* [x] **Docker & Compose:** Multi-stage production container builds.

> **The Difference:**  
> Anyone can say: *"I built a Kafka project."*  
> This project proves: *"I built a fault-tolerant, high-concurrency event-driven platform capable of surviving enterprise production failures."*

---

## Recommended Implementation Order

```text
PHASE 1 ──► Order Service + PostgreSQL + REST APIs (COMPLETED)
   ↓
PHASE 2 ──► Payment Service + Inventory Service (COMPLETED)
   ↓
PHASE 3 ──► Kafka Event Bus + Schemas + State Machine (COMPLETED)
   ↓
PHASE 4 ──► Transactional Outbox Pattern (COMPLETED)
   ↓
PHASE 5 ──► Idempotency + Retries + Dead Letter Queue (COMPLETED)
   ↓
PHASE 6 ──► Concurrency & Failure / Chaos Testing (COMPLETED)
   ↓
PHASE 7 ──► Redis + Observability + Docker (COMPLETED)
   ↓
PHASE 8 ──► Load Testing + Final Documentation (QUEUED)
```

---

## Quickstart & Running Tests

### Prerequisites
* Python 3.11+
* Docker & Docker Compose

### 1. Environment Setup
```bash
cp .env.example .env
```

### 2. Launch Infrastructure (Docker)
```bash
docker-compose -f docker-compose.dev.yml up -d
```

### 3. Run Automated Tests
```bash
# Shared package tests (10 tests)
pytest backend/shared/tests/ -v

# Order Service tests (20 tests)
cd backend/services/order-service
pytest tests/integration/ -v

# Payment Service tests (14 tests)
cd ../payment-service
pytest tests/integration/ -v

# Inventory Service tests (13 tests)
cd ../inventory-service
pytest tests/integration/ -v
```

**Total Test Suite: 57 tests passing with 100% green rate.**
