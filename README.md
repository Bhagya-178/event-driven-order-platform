Event-Driven Order & Payment Platform

Goal: Build a realistic e-commerce backend where orders, payments, inventory, and notifications communicate through Kafka events.

Core stack
Python + FastAPI
PostgreSQL
Kafka
Redis
Docker / Docker Compose
SQLAlchemy + Alembic
Pydantic
pytest
Prometheus + OpenTelemetry — optional after core functionality
1. High-level project diagram
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
2. The actual business flow

This is the important part of the project.

A customer does:

POST /orders

Example:

{
  "customer_id": 101,
  "items": [
    {
      "product_id": 501,
      "quantity": 2
    }
  ]
}

Then:

Client
  │
  ▼
Order Service
  │
  ├── Validate request
  ├── Check product
  ├── Calculate price
  ├── Create order
  └── Create Outbox Event
          │
          ▼
       Kafka
          │
          ▼
   OrderCreated
      │
      ├───────────────┐
      ▼               ▼
 Payment Service   Inventory Service
      │               │
      ▼               ▼
 Payment             Reserve
 Processing          Stock
      │               │
      ▼               ▼
PaymentSucceeded  InventoryReserved
      │               │
      └───────┬───────┘
              ▼
            Kafka
              │
              ▼
         Order Service
              │
              ▼
        CONFIRMED
3. Failure flow

This is where your project becomes interesting.

Suppose payment fails:

OrderCreated
     │
     ▼
 Payment Service
     │
     ▼
 Payment Failed
     │
     ▼
 PaymentFailed event
     │
     ▼
    Kafka
     │
     ▼
 Order Service
     │
     ▼
ORDER = PAYMENT_FAILED

Then inventory reservation can be released.

PaymentFailed
      │
      ▼
Inventory Service
      │
      ▼
Release Reservation
4. Project structure

I recommend a monorepo with separate services.

event-driven-order-platform/backend
│
├── services/
│
│   ├── order-service/
│   │   ├── app/
│   │   │   ├── api/
│   │   │   │   ├── routes/
│   │   │   │   │   ├── orders.py
│   │   │   │   │   └── health.py
│   │   │   │   └── dependencies.py
│   │   │   │
│   │   │   ├── core/
│   │   │   │   ├── config.py
│   │   │   │   ├── logging.py
│   │   │   │   └── security.py
│   │   │   │
│   │   │   ├── models/
│   │   │   │   ├── order.py
│   │   │   │   ├── order_item.py
│   │   │   │   └── outbox.py
│   │   │   │
│   │   │   ├── schemas/
│   │   │   │   ├── order.py
│   │   │   │   └── events.py
│   │   │   │
│   │   │   ├── services/
│   │   │   │   ├── order_service.py
│   │   │   │   └── pricing_service.py
│   │   │   │
│   │   │   ├── repositories/
│   │   │   │   ├── order_repository.py
│   │   │   │   └── outbox_repository.py
│   │   │   │
│   │   │   ├── messaging/
│   │   │   │   ├── producer.py
│   │   │   │   └── consumers.py
│   │   │   │
│   │   │   ├── db/
│   │   │   │   ├── session.py
│   │   │   │   └── base.py
│   │   │   │
│   │   │   └── main.py
│   │   │
│   │   ├── tests/
│   │   │   ├── unit/
│   │   │   ├── integration/
│   │   │   └── api/
│   │   │
│   │   ├── alembic/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   │
│   ├── payment-service/
│   │   ├── app/
│   │   │   ├── api/
│   │   │   ├── core/
│   │   │   ├── models/
│   │   │   ├── schemas/
│   │   │   ├── services/
│   │   │   ├── repositories/
│   │   │   ├── messaging/
│   │   │   ├── db/
│   │   │   └── main.py
│   │   │
│   │   ├── tests/
│   │   ├── alembic/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   │
│   ├── inventory-service/
│   │   ├── app/
│   │   │   ├── api/
│   │   │   ├── core/
│   │   │   ├── models/
│   │   │   ├── schemas/
│   │   │   ├── services/
│   │   │   ├── repositories/
│   │   │   ├── messaging/
│   │   │   ├── db/
│   │   │   └── main.py
│   │   │
│   │   ├── tests/
│   │   ├── alembic/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   │
│   └── notification-service/
│       ├── app/
│       │   ├── core/
│       │   ├── messaging/
│       │   ├── services/
│       │   └── main.py
│       │
│       ├── tests/
│       ├── Dockerfile
│       └── pyproject.toml
│
│
├── shared/
│   ├── events/
│   │   ├── event_types.py
│   │   └── schemas.py
│   │
│   ├── observability/
│   │   ├── logging.py
│   │   └── tracing.py
│   │
│   └── exceptions/
│       └── errors.py
│
│
├── infrastructure/
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   └── docker-compose.test.yml
│   │
│   ├── kafka/
│   │   ├── topics.sh
│   │   └── config/
│   │
│   ├── postgres/
│   │   └── init.sql
│   │
│   └── redis/
│
│
├── tests/
│   ├── contract/
│   ├── e2e/
│   ├── failure/
│   └── concurrency/
│
│
├── docs/
│   ├── architecture.md
│   ├── event-catalog.md
│   ├── api.md
│   └── failure-scenarios.md
│
├── .env.example
├── .gitignore
├── Makefile
├── docker-compose.yml
├── README.md
└── LICENSE
5. Don't make 10 microservices

This is important.

You don't need:

❌ Customer Service
❌ Product Service
❌ Cart Service
❌ Pricing Service
❌ Order Service
❌ Payment Service
❌ Inventory Service
❌ Shipping Service
❌ Notification Service
❌ Analytics Service

That is scope creep.

For your project:

Order Service
Payment Service
Inventory Service
Notification Service

is enough.

And even Notification can be extremely small.

6. Kafka topics

Use a small event catalogue.

orders.events
payments.events
inventory.events
notifications.events

Events:

OrderCreated
OrderCancelled

PaymentRequested
PaymentSucceeded
PaymentFailed
RefundRequested
RefundSucceeded

InventoryReservationRequested
InventoryReserved
InventoryReservationFailed
InventoryReleased

You should define a standard event envelope:

{
  "event_id": "uuid",
  "event_type": "OrderCreated",
  "aggregate_id": "order-123",
  "occurred_at": "2026-09-02T10:30:00Z",
  "correlation_id": "uuid",
  "version": 1,
  "payload": {}
}

This gives you a very good interview topic:

Why do we need event_id, correlation_id, and version?

7. Database design
Order DB
orders
----------------
id
customer_id
status
total_amount
currency
created_at
updated_at
version
order_items
----------------
id
order_id
product_id
quantity
unit_price
outbox_events
----------------
id
event_id
aggregate_id
event_type
payload
status
created_at
published_at
retry_count
Payment DB
payments
----------------
id
order_id
idempotency_key
amount
currency
status
provider_reference
created_at
updated_at
Inventory DB
inventory
----------------
product_id
available_quantity
reserved_quantity
version
updated_at
8. Critical feature: Idempotency

This should absolutely be included.

Imagine Kafka delivers:

PaymentSucceeded

twice.

Without idempotency:

PaymentSucceeded
      ↓
Update order
      ↓
Update order again
      ↓
Send receipt again

Your consumer should detect:

event_id already processed

and safely ignore the duplicate.

For payment APIs, also use:

Idempotency-Key: abc-123

This prevents:

Customer clicks Pay
       ↓
Network timeout
       ↓
Customer clicks Pay again
       ↓
💥 Two payments
9. Critical feature: Transactional Outbox

This is one of the most valuable parts of the project.

Instead of:

DB transaction
     ↓
Kafka publish

do:

BEGIN TRANSACTION

Create Order

Create Outbox Event

COMMIT

Then:

Outbox Publisher
       │
       ▼
Read unpublished events
       │
       ▼
Kafka
       │
       ▼
Mark event published

So:

PostgreSQL
┌───────────────────────┐
│ orders                │
│ outbox_events         │
└───────────┬───────────┘
            │
            ▼
     Outbox Publisher
            │
            ▼
          Kafka

This is much more impressive than simply calling Kafka from your API handler.

10. Retry + DLQ

Suppose Payment Service receives:

OrderCreated

but its payment provider is temporarily unavailable.

Don't immediately fail permanently.

Attempt 1
   ↓
FAIL
   ↓
2 seconds
   ↓
Attempt 2
   ↓
FAIL
   ↓
5 seconds
   ↓
Attempt 3
   ↓
FAIL
   ↓
      DLQ

Then:

Kafka
  │
  ▼
Payment Consumer
  │
  ├── Success → continue
  │
  └── Failure
         │
         ▼
       Retry
         │
         ▼
        DLQ

You should be able to explain:

At-least-once delivery means duplicate processing is possible, therefore consumers must be idempotent.

That's exactly the kind of distributed-systems reasoning you want to demonstrate.

11. Order state machine

Don't allow random status changes.

For example:

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

Your service should reject invalid transitions.

For example:

DELIVERED → CREATED

should never be allowed.

12. What makes this project production-level?

Don't judge it by number of services.

Judge it by whether you handle:

✓ Transactions
✓ Idempotency
✓ Event ordering
✓ Duplicate events
✓ Retries
✓ Dead-letter queue
✓ Transactional outbox
✓ Concurrency
✓ Database consistency
✓ Failure recovery
✓ Correlation IDs
✓ Structured logging
✓ Health checks
✓ API validation
✓ Authentication
✓ Authorization
✓ Rate limiting
✓ Automated tests
✓ Docker

That's the difference between:

"I built a Kafka project."

and:

"I built a fault-tolerant event-driven order processing system."

Recommended implementation order

Don't start by building everything.

PHASE 1
Order Service
PostgreSQL
REST APIs
        ↓

PHASE 2
Payment Service
Inventory Service
        ↓

PHASE 3
Kafka
Events
Consumers
        ↓

PHASE 4
Transactional Outbox
        ↓

PHASE 5
Idempotency
Retries
DLQ
        ↓

PHASE 6
Concurrency + failure testing
        ↓

PHASE 7
Redis
Observability
Docker
        ↓

PHASE 8
Load testing
Documentation
