# Operational Runbook & Disaster Recovery Guide

<p align="center">
  <b>Event-Driven Order & Payment Platform &mdash; SRE & Incident Response Runbook</b>
</p>

---

## 1. Executive Summary & Incident Response Protocol

This runbook defines standard operating procedures (SOP), forensic diagnostic steps, and disaster recovery playbooks for operators, SREs, and on-call engineers supporting the Event-Driven Order Platform.

### Severity Levels & Escalation Matrix

| Severity | Definition | Target MTTR | Action |
| :--- | :--- | :---: | :--- |
| **SEV-1** | Outbox relay halt across all services, or total database connection starvation. | `< 15 min` | Engage Primary On-Call, activate fail-open fallbacks, notify stakeholders. |
| **SEV-2** | Messages actively routing to DLQs (`*.events.dlq`), or Redis cluster failure causing fail-open degraded mode. | `< 45 min` | Investigate poison pill payloads, monitor DB read loads, inspect consumer lags. |
| **SEV-3** | Individual consumer group rebalance or transient rate limiting spikes. | `< 2 hours` | Scale consumers or adjust sliding-window burst allowances. |

---

## 2. Dead-Letter Queue (DLQ) Forensics & Replay Playbook

### 2.1 Why Messages Route to DLQ
Every consumer across the platform wraps message processing inside the resilient `ConsumerEventHandler` and `process_with_retry_and_dlq` handlers. A message is routed to `*.events.dlq` under two conditions:
1. **Poison Pill:** An unparseable or corrupted payload that immediately fails Pydantic schema validation.
2. **Exhausted Retries:** A transient database failure, downstream HTTP timeout, or constraint violation that fails 3 consecutive attempts with exponential backoff.

> **Zero Head-of-Line Blocking Guarantee:**  
> When a message is dispatched to the DLQ, the consumer **commits the Kafka offset immediately**, preventing the bad message from halting subsequent orders on that partition.

### 2.2 Forensic Inspection Schema
Messages in the DLQ conform to the standardized `DeadLetterEnvelope`:

```json
{
  "dlq_id": "8f3b23e1-512a-4a27-a02b-2f3b9c0d1e2f",
  "original_topic": "orders.events",
  "original_partition": 1,
  "original_offset": 1042,
  "original_key": "order_7f8a9b1c",
  "error_type": "ValidationError",
  "error_message": "Invalid currency: 'XYZ' not in allowed ISO list",
  "stack_trace": "Traceback (most recent call last):\n  File 'consumer.py'...",
  "retry_count": 3,
  "failed_at": "2026-09-05T00:15:00Z",
  "payload": { ... }
}
```

### 2.3 Inspecting DLQ Topics
To inspect the latest dead-lettered messages in real-time:

```bash
# Order Service DLQ
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.events.dlq \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true

# Payment Service DLQ
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payments.events.dlq \
  --from-beginning

# Inventory Service DLQ
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic inventory.events.dlq \
  --from-beginning
```

### 2.4 Step-by-Step DLQ Replay Procedure
Once the underlying bug or downstream dependency is fixed, replay quarantined messages back to the primary topic:

1. **Dump Quarantined Messages to File:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic orders.events.dlq \
     --from-beginning \
     --timeout-ms 10000 > dlq_dump.json
   ```
2. **Filter & Extract Original Payloads:**
   Extract the inner `"payload"` object from each `DeadLetterEnvelope`.
3. **Republish to Primary Topic:**
   Republish extracted payloads to `orders.events` using the original partition key (`aggregate_id`). Consumer idempotency tables (`processed_events`) guarantee that any previously completed steps will not be duplicated.

---

## 3. Transactional Outbox Scaling & Broker Outages

### 3.1 Multi-Worker Concurrency (`SKIP LOCKED`)
Outbox event relaying utilizes PostgreSQL's row-level locking with `SKIP LOCKED`:

```sql
SELECT * FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

- **Scale Horizontally:** You can safely run 10+ concurrent `OutboxPublisher` processes per service without race conditions or duplicate event publications.
- **Tuning Throughput:** Adjust `BATCH_SIZE` (default: 50) and `POLL_INTERVAL_SECONDS` (default: 0.5s) in service configurations.

### 3.2 Kafka Broker Outage Recovery
If Apache Kafka is completely offline or unreachable:

1. **Application Continues Undisrupted:** API requests still succeed with `HTTP 201 Created` because business data and outbox events are committed together in the local PostgreSQL database transaction.
2. **Outbox Retries:** The outbox publisher catches connection errors, records `last_error`, increments `retry_count`, and keeps records in `PENDING` state.
3. **Automatic Self-Healing:** Once Kafka recovers, the polling loop automatically drains the pending queue in FIFO chronological order. No manual intervention is required.

---

## 4. Redis Fail-Open Degradation & Recovery

### 4.1 Fail-Open Resilience Behavior
The platform implements an automatic **Fail-Open Strategy** via `RedisManager`:

```text
Incoming GET /orders/{id}
         │
         ▼
  Query Redis Cache
         │
    [Redis Offline / Error]
         │
         ├────────────────────────┐
         │ (Catches Exception)    │
         ▼                        ▼
   Log Warning              Bypass Cache
(Prometheus metric inc)          │
                                 ▼
                         Fetch from PostgreSQL
                                 │
                                 ▼
                         Return HTTP 200 OK
```

- **No 500 Outages:** The platform **never returns HTTP 500 errors** due to Redis outages.
- **Rate Limiting Degradation:** If Redis is down, rate limiting degrades safely to allow traffic through rather than denying legitimate customers.

### 4.2 Restoring Redis & Cache Flush
When recovering a failed Redis instance:

1. **Verify Connectivity:**
   ```bash
   redis-cli -h localhost -p 6379 ping
   # Expected output: PONG
   ```
2. **Purge Corrupted or Desynchronized Keys:**
   ```bash
   # Remove all cached orders to force fresh reads from PostgreSQL
   redis-cli -h localhost -p 6379 EVAL "return redis.call('del', unpack(redis.call('keys', 'order:*')))" 0
   ```
3. **Verify Service Health:**
   Call `/health/ready` on each microservice to ensure deep database and Redis checks return HTTP 200.

---

## 5. High-Contention Inventory & Concurrency Troubleshooting

### 5.1 Optimistic Locking Conflicts (`StaleDataError`)
- **Symptom:** Client receives `HTTP 409 Conflict` during flash-sale stock reservations.
- **Cause:** Two concurrent transactions attempted to reserve the same inventory item at the same microsecond; the second transaction detected a modified `version` column.
- **Remediation:** The platform already incorporates an automatic 10-attempt exponential retry loop in `InventoryService.reserve_stock`. If 409 persists, client applications should apply randomized jittered retries.

### 5.2 Investigating PostgreSQL Connection Pool Starvation
If service logs display `QueuePool limit of size 5 overflow 10 reached`:

1. **Check Active PostgreSQL Connections:**
   ```sql
   SELECT pid, usename, client_addr, state, query_start, query 
   FROM pg_stat_activity 
   WHERE state != 'idle' 
   ORDER BY query_start ASC;
   ```
2. **Terminate Hanging Sessions:**
   ```sql
   SELECT pg_terminate_backend(pid) 
   FROM pg_stat_activity 
   WHERE state = 'idle in transaction' AND query_start < NOW() - INTERVAL '5 minutes';
   ```

---

## 6. SRE Production Checklist & Sizing Guidelines

### 6.1 Kafka Configuration
- **Partitioning:** Set `--partitions 6` or `--partitions 12` on `orders.events`, `payments.events`, and `inventory.events` to match expected consumer scaling units.
- **Entity Keying:** Always publish events using `aggregate_id` (e.g. `order_id`) as the partition key to enforce strict chronological per-order message ordering.
- **Retention:** Primary event topics: `7 days` (`retention.ms=604800000`). DLQ topics: `30 days`.

### 6.2 Service Sizing & Connection Pools
| Component | Minimum Replicas | CPU / Memory Request | DB Pool Size | Notes |
| :--- | :---: | :---: | :---: | :--- |
| `order-service` | 2 | 0.5 CPU / 512 MB | 10 (max 20) | High read traffic, Redis cache enabled. |
| `payment-service` | 2 | 0.5 CPU / 512 MB | 5 (max 10) | Event-driven consumer worker + Outbox. |
| `inventory-service` | 2 | 0.5 CPU / 512 MB | 10 (max 20) | Pessimistic and optimistic lock intensive. |
| `PostgreSQL 15` | 1 (Primary + Replica) | 2 CPU / 4 GB | `max_connections = 200` | SSD storage with WAL write-ahead logging. |
| `Redis 7` | 1 (Cluster mode prod) | 1 CPU / 2 GB | `maxmemory 1.5gb` | Policy: `volatile-lru`. |

---

## 7. Recommended Prometheus Alerts

Configure these alerting rules in [`backend/infrastructure/prometheus/prometheus.yml`](file:///e:/Project/event-driven-order-platform/backend/infrastructure/prometheus/prometheus.yml):

1. **High DLQ Routing Rate:**
   ```promql
   sum(rate(dlq_events_routed_total[5m])) > 0.05
   # Trigger SEV-2 alert: Poison pills or repeated consumer failures detected.
   ```
2. **Elevated Consumer Lag:**
   ```promql
   kafka_consumergroup_lag{topic="orders.events"} > 500
   # Trigger warning: Consumer group is falling behind event production.
   ```
3. **Redis Health Degraded (Fail-Open Active):**
   ```promql
   http_requests_total{status_code="503"} > 0 or probe_success{instance=~".*redis.*"} == 0
   # Notify SRE: Cache is running in degraded fail-open mode.
   ```
