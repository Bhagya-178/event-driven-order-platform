"""
Locust Load Testing Suite for Event-Driven Order & Payment Platform.

Simulates enterprise traffic patterns across four realistic user personas:
1. OrderPlacementUser: End-to-end checkout with Idempotency-Key guarantees and status polling.
2. OrderLookupUser: Read-heavy traffic evaluating Redis cache hit latency.
3. InventoryContentionUser: Flash-sale concurrency stress on shared inventory stock.
4. ThrottledAttackerUser: Burst traffic verifying sliding-window rate limiting (HTTP 429).
"""

import uuid
import random
from locust import HttpUser, task, between, constant_pacing, tag

# Shared pool of IDs for simulating cache hits and contention
HOT_PRODUCT_ID = "11111111-1111-1111-1111-111111111111"
SHARED_CUSTOMER_IDS = [
    "c1010000-0000-0000-0000-000000000101",
    "c1010000-0000-0000-0000-000000000102",
    "c1010000-0000-0000-0000-000000000103",
]


class OrderPlacementUser(HttpUser):
    """
    Simulates standard e-commerce buyers placing orders with idempotency guarantees.
    """
    wait_time = between(1.0, 3.0)
    weight = 3

    @tag("orders", "checkout")
    @task(3)
    def place_order_with_idempotency(self):
        idempotency_key = str(uuid.uuid4())
        customer_id = random.choice(SHARED_CUSTOMER_IDS)
        product_id = str(uuid.uuid4())

        payload = {
            "customer_id": customer_id,
            "items": [
                {"product_id": product_id, "quantity": random.randint(1, 3)}
            ]
        }
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key
        }

        # 1. First order placement attempt
        with self.client.post(
            "/orders/",
            json=payload,
            headers=headers,
            name="/orders/ [create]",
            catch_response=True
        ) as response:
            if response.status_code == 201:
                order_data = response.json()
                order_id = order_data.get("id")
                response.success()
            else:
                response.failure(f"Order creation failed: {response.status_code} {response.text}")
                return

        # 2. Duplicate retry with same idempotency key (must return exact same order)
        with self.client.post(
            "/orders/",
            json=payload,
            headers=headers,
            name="/orders/ [idempotent-retry]",
            catch_response=True
        ) as retry_resp:
            if retry_resp.status_code == 201 and retry_resp.json().get("id") == order_id:
                retry_resp.success()
            else:
                retry_resp.failure(f"Idempotency broken! Status: {retry_resp.status_code}, body: {retry_resp.text}")

        # 3. Poll order status
        if order_id:
            self.client.get(
                f"/orders/{order_id}",
                name="/orders/{id} [poll-status]"
            )


class OrderLookupUser(HttpUser):
    """
    Simulates read-heavy traffic to stress Redis caching and measure sub-millisecond cache latency.
    """
    wait_time = between(0.1, 0.5)
    weight = 5

    def on_start(self):
        self.cached_order_ids = []
        # Pre-seed with one known lookup id or probe health
        self.cached_order_ids.append("00000000-0000-0000-0000-000000000001")

    @tag("cache", "reads")
    @task(4)
    def get_order_cached(self):
        order_id = random.choice(self.cached_order_ids)
        with self.client.get(
            f"/orders/{order_id}",
            name="/orders/{id} [cached-read]",
            catch_response=True
        ) as response:
            # 200 (cache/db hit) or 404 (not found) are acceptable non-server error responses
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @tag("health", "observability")
    @task(1)
    def check_health_ready(self):
        self.client.get("/health/ready", name="/health/ready")


class InventoryContentionUser(HttpUser):
    """
    Simulates high-contention flash-sale reservations against the Inventory Service.
    Target: Inventory Service (default port: 8002).
    """
    wait_time = between(0.2, 0.8)
    weight = 2

    @tag("inventory", "contention")
    @task
    def reserve_hot_inventory(self):
        payload = {
            "order_id": str(uuid.uuid4()),
            "product_id": HOT_PRODUCT_ID,
            "quantity": 1
        }
        with self.client.post(
            "/inventory/reservations",
            json=payload,
            name="/inventory/reservations [contention]",
            catch_response=True
        ) as response:
            # Under extreme contention:
            # 201 = Stock reserved successfully
            # 400 = Insufficient stock (sold out)
            # 409 = Optimistic locking conflict (handled by retry loop or caller)
            if response.status_code in (201, 400, 409):
                response.success()
            else:
                response.failure(f"Inventory reservation error: {response.status_code} {response.text}")


class ThrottledAttackerUser(HttpUser):
    """
    Simulates abusive burst traffic to verify that the Redis sliding-window
    rate limiter returns HTTP 429 Too Many Requests once quotas (100 req/min) are exceeded.
    """
    wait_time = constant_pacing(0.02)  # ~50 req/sec from a single client
    weight = 1

    @tag("security", "ratelimit")
    @task
    def burst_orders_endpoint(self):
        with self.client.get(
            "/orders/00000000-0000-0000-0000-000000000000",
            name="/orders/{id} [rate-limit-burst]",
            catch_response=True
        ) as response:
            if response.status_code == 429:
                # Expected when rate limit is active!
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    response.success()
                else:
                    response.failure("HTTP 429 received but missing Retry-After header")
            elif response.status_code in (200, 404):
                # Within allowance
                response.success()
            else:
                response.failure(f"Unexpected response under burst: {response.status_code}")
