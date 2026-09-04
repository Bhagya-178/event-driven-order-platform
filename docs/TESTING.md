# Comprehensive Testing Guide & Verification Playbook

<p align="center">
  <b>Event-Driven Order & Payment Platform &mdash; Test Suite Architecture & Execution Reference</b>
</p>

---

## 1. Test Suite Overview

The platform is fortified with an exhaustive automated test suite consisting of **55 tests** across 4 packages, verifying data consistency, race-condition defenses, transactional outbox relays, DLQ isolation, and chaos recovery.

### Test Metrics Summary

| Package / Microservice | Path | Automated Passing | Skipped (Live Cluster) | Total Tests |
| :--- | :--- | :---: | :---: | :---: |
| **Shared Foundation** | `backend/shared/tests/` | **10** | 0 | **10** |
| **Order Service** | `backend/services/order-service/tests/` | **20** | 1 | **21** |
| **Payment Service** | `backend/services/payment-service/tests/` | **11** | 0 | **11** |
| **Inventory Service** | `backend/services/inventory-service/tests/` | **13** | 0 | **13** |
| **GRAND TOTAL** | &mdash; | **54** | **1** | **55** |

---

## 2. Environment Configuration & Prerequisites

### 2.1 Python Environment
* Python 3.11+ or 3.14
* Dependencies installed in the virtual environment (located at `backend/services/order-service/.venv/`):
  - `fastapi`, `sqlalchemy`, `aiosqlite`, `pytest`, `pytest-asyncio`, `httpx`, `pydantic`.

### 2.2 Environment Variables
When running unit/integration tests without a live PostgreSQL cluster, services use in-memory SQLite (`sqlite+aiosqlite:///:memory:`):

| Environment Variable | Target Service | Default Test Value |
| :--- | :--- | :--- |
| `PYTHONPATH` | All services | `backend/shared;backend/services/<service-name>` |
| `ORDER_DB_URL` | Order Service | `sqlite+aiosqlite:///:memory:` |
| `PAYMENT_DB_URL` | Payment Service | `sqlite+aiosqlite:///:memory:` |
| `INVENTORY_DB_URL` | Inventory Service | `sqlite+aiosqlite:///:memory:` |

---

## 3. One-Click Test Runners (All 55 Tests)

### 3.1 PowerShell (Windows)
Run from the root of the repository:

```powershell
Write-Host "=== 1. SHARED FOUNDATION (10 Tests) ===" -ForegroundColor Cyan
$env:PYTHONPATH="backend/shared"
& "backend/services/order-service/.venv/Scripts/python.exe" -m pytest backend/shared/tests/ -q

Write-Host "`n=== 2. ORDER SERVICE (21 Tests) ===" -ForegroundColor Cyan
$env:PYTHONPATH="backend/shared;backend/services/order-service"
$env:ORDER_DB_URL="sqlite+aiosqlite:///:memory:"
& "backend/services/order-service/.venv/Scripts/python.exe" -m pytest backend/services/order-service/tests/ -q

Write-Host "`n=== 3. PAYMENT SERVICE (11 Tests) ===" -ForegroundColor Cyan
$env:PYTHONPATH="backend/shared;backend/services/payment-service"
$env:PAYMENT_DB_URL="sqlite+aiosqlite:///:memory:"
& "backend/services/order-service/.venv/Scripts/python.exe" -m pytest backend/services/payment-service/tests/ -q

Write-Host "`n=== 4. INVENTORY SERVICE (13 Tests) ===" -ForegroundColor Cyan
$env:PYTHONPATH="backend/shared;backend/services/inventory-service"
$env:INVENTORY_DB_URL="sqlite+aiosqlite:///:memory:"
& "backend/services/order-service/.venv/Scripts/python.exe" -m pytest backend/services/inventory-service/tests/ -q
```

### 3.2 Bash (Linux / macOS / WSL)
```bash
# 1. Shared Foundation
PYTHONPATH="backend/shared" pytest backend/shared/tests/ -q

# 2. Order Service
PYTHONPATH="backend/shared:backend/services/order-service" \
ORDER_DB_URL="sqlite+aiosqlite:///:memory:" \
pytest backend/services/order-service/tests/ -q

# 3. Payment Service
PYTHONPATH="backend/shared:backend/services/payment-service" \
PAYMENT_DB_URL="sqlite+aiosqlite:///:memory:" \
pytest backend/services/payment-service/tests/ -q

# 4. Inventory Service
PYTHONPATH="backend/shared:backend/services/inventory-service" \
INVENTORY_DB_URL="sqlite+aiosqlite:///:memory:" \
pytest backend/services/inventory-service/tests/ -q
```

---

## 4. Running Tests by Service

### 4.1 Shared Foundation Package
```bash
cd backend/shared
pytest tests/ -v
```
**Tests Covered:**
- Event Envelope schema validation and serialization.
- Causation & Correlation ID propagation.
- DeadLetterEnvelope forensic payload validation.
- Exponential backoff retry loop with DLQ dispatch.
- RedisManager fail-open degradation mechanics.
- Sliding-window rate limiting quota enforcement.
- Structured JSON logging formatter.
- Prometheus `/metrics` exposition collector.

### 4.2 Order Service
```bash
cd backend/services/order-service
pytest tests/ -v
```
**Tests Covered:**
- Multi-worker transactional outbox polling with `SKIP LOCKED` (30 events, 3 workers).
- 10 concurrent HTTP requests with identical `Idempotency-Key` (verifying exact single order creation).
- Cross-topic asynchronous race safety (`PaymentSucceeded` + `InventoryReserved`).
- Poison pill isolation and partition unblocking (`orders.events.dlq`).
- Broker failure and self-healing recovery.
- Redis caching with automatic key invalidation on state transitions.
- Sliding-window rate limiter HTTP 429 enforcement.

### 4.3 Payment Service
```bash
cd backend/services/payment-service
pytest tests/ -v
```
**Tests Covered:**
- Multi-worker payment outbox concurrency with `SKIP LOCKED`.
- Concurrent payment API idempotency races.
- Poison pill unblocking on `payments.events.dlq`.
- At-least-once redelivery deduplication via `processed_events`.
- Transient gateway failure retries and DLQ routing.
- Deep `/health/ready` probe checking PostgreSQL connectivity.

### 4.4 Inventory Service
```bash
cd backend/services/inventory-service
pytest tests/ -v
```
**Tests Covered:**
- 20 concurrent reservation requests competing for 5 stock units (zero overselling guarantee).
- Optimistic locking collision detection (`StaleDataError` retry loop).
- Stock reservation, insufficient stock rejection (HTTP 400), and release compensation.
- Poison pill isolation on `inventory.events.dlq`.
- Outbox relaying to `inventory.events`.

---

## 5. Running Tests by Distributed Systems Category

Run cross-service tests targeting specific engineering guarantees using pytest expression matching (`-k`):

### 5.1 Concurrency & Race Condition Suite
```powershell
# Run all concurrency tests across Order, Payment, and Inventory services
pytest backend/services/order-service/tests/integration/test_concurrency.py -v
pytest backend/services/payment-service/tests/integration/test_concurrency.py -v
pytest backend/services/inventory-service/tests/integration/test_concurrency.py -v
```

### 5.2 Chaos & Failure Recovery Suite
```powershell
# Run poison pill, broker outage, and crash recovery tests
pytest backend/services/order-service/tests/integration/test_failure.py -v
pytest backend/services/payment-service/tests/integration/test_failure.py -v
pytest backend/services/inventory-service/tests/integration/test_failure.py -v
```

### 5.3 Resilience & Dead-Letter Queue Suite
```powershell
# Run exponential retry loops and dead-letter routing tests
pytest backend/shared/tests/test_schemas.py -k "retry_and_dlq" -v
pytest backend/services/order-service/tests/integration/test_resilience.py -v
pytest backend/services/payment-service/tests/integration/test_resilience.py -v
pytest backend/services/inventory-service/tests/integration/test_resilience.py -v
```

### 5.4 Transactional Outbox Suite
```powershell
# Run atomic outbox insertion and multi-worker polling tests
pytest backend/services/order-service/tests/integration/test_outbox.py -v
pytest backend/services/payment-service/tests/integration/test_outbox.py -v
pytest backend/services/inventory-service/tests/integration/test_outbox.py -v
```

---

## 6. Targeted Test Execution & Debugging Tips

### 6.1 Run a Single Test Function
```powershell
pytest backend/services/inventory-service/tests/integration/test_concurrency.py::test_concurrent_inventory_reservations_overselling_prevention -v
```

### 6.2 Show Live Output & Print Statements (`-s`)
```powershell
pytest backend/services/order-service/tests/integration/test_concurrency.py -s -v
```

### 6.3 Stop on First Failure (`-x`)
```powershell
pytest backend/services/order-service/tests/ -x
```

---

## 7. In-Memory SQLite Concurrency vs PostgreSQL

### How Test Fixtures Simulate Row-Level Locks:
In tests, services run on `sqlite+aiosqlite:///:memory:` with SQLAlchemy's `StaticPool` to avoid needing a live PostgreSQL cluster for fast local feedback.

Because SQLite shares a single underlying connection across all sessions in memory, concurrent transactions cannot perform native row-level pessimistic locking (`SELECT ... FOR UPDATE`) without throwing `OperationalError: database is locked`.

To solve this, services implement a conditional dialect check:
```python
is_postgres = self.session.bind and self.session.bind.dialect.name == "postgresql"
if not is_postgres:
    await self._sqlite_lock.acquire()
try:
    # Transactional logic...
finally:
    if not is_postgres:
        self._sqlite_lock.release()
```
- **In Unit Tests (SQLite):** Serializes critical sections via `_sqlite_lock`, allowing concurrency test suites (e.g. 20 concurrent reservation requests) to test race logic cleanly.
- **In Production (PostgreSQL):** Bypasses `_sqlite_lock` entirely to use native PostgreSQL row-level pessimistic locks (`SELECT ... FOR UPDATE`) and optimistic versioning (`version = version + 1`).

---

## 8. Live Broker Integration Testing

One test ([`test_kafka_consumer.py`](file:///e:/Project/event-driven-order-platform/backend/services/order-service/tests/integration/test_kafka_consumer.py)) is skipped during unit testing because it connects to an active Kafka broker.

To run this test against live infrastructure:

1. **Start the Kafka cluster:**
   ```bash
   docker-compose -f docker-compose.dev.yml up -d
   ```
2. **Execute the integration test:**
   ```powershell
   $env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
   pytest backend/services/order-service/tests/integration/test_kafka_consumer.py -v
   ```
