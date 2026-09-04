import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.redis import redis_manager
from app.models.order import Order
from app.db.session import AsyncSessionLocal
from shared.events.schemas import EventEnvelope
from app.messaging.consumer import order_consumer

@pytest.mark.asyncio
async def test_metrics_endpoint_scraped(async_client):
    response = await async_client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "http_requests_total" in body
    assert "http_request_duration_seconds" in body

@pytest.mark.asyncio
async def test_health_live_and_ready(async_client):
    # Live probe
    live_resp = await async_client.get("/health/live")
    assert live_resp.status_code == 200
    assert live_resp.json() == {"status": "ok"}

    # Ready probe
    ready_resp = await async_client.get("/health/ready")
    assert ready_resp.status_code == 200
    data = ready_resp.json()
    assert data["status"] == "ready"
    assert data["database"] == "up"

@pytest.mark.asyncio
async def test_order_redis_caching_and_invalidation():
    from app.db.session import engine
    from app.db.base import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    order_id = uuid4()
    # 1. Seed Order in DB
    async with AsyncSessionLocal() as session:
        order = Order(
            id=order_id,
            customer_id=uuid4(),
            total_amount=100.00,
            currency="USD",
            status="CREATED",
            payment_status="PENDING",
            inventory_status="PENDING"
        )
        session.add(order)
        await session.commit()

    cache_key = f"order:{order_id}"
    await redis_manager.delete(cache_key)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First request: DB query -> cached into Redis
        resp1 = await client.get(f"/orders/{order_id}")
        assert resp1.status_code == 200
        assert resp1.json()["status"] == "CREATED"

        # Verify cached in redis (or if redis offline, verify gracefully succeeds)
        cached = await redis_manager.get(cache_key)
        if redis_manager.client:
            assert cached is not None

        # 2. Simulate consumer updating order state to CONFIRMED
        # Consumer commits and invalidates cache
        payment_event = EventEnvelope(
            event_id=uuid4(),
            event_type="PaymentSucceeded",
            aggregate_type="order",
            aggregate_id=order_id,
            correlation_id=uuid4(),
            payload={"amount": "100.00", "currency": "USD", "status": "SUCCEEDED"}
        )
        inventory_event = EventEnvelope(
            event_id=uuid4(),
            event_type="InventoryReserved",
            aggregate_type="order",
            aggregate_id=order_id,
            correlation_id=uuid4(),
            payload={"items": []}
        )
        await order_consumer.process_event(payment_event)
        await order_consumer.process_event(inventory_event)

        # Cache key should be invalidated
        if redis_manager.client:
            assert await redis_manager.get(cache_key) is None

        # Second request: fetches updated CONFIRMED state from DB and recaches
        resp2 = await client.get(f"/orders/{order_id}")
        assert resp2.status_code == 200
        assert resp2.json()["status"] == "CONFIRMED"

@pytest.mark.asyncio
async def test_rate_limiting_enforcement():
    # Test rate limiter logic
    from shared.redis.rate_limiter import RateLimiter
    mock_redis = type("MockRedis", (), {
        "client": type("Client", (), {"ttl": AsyncMock(return_value=30)})(),
        "increment": AsyncMock(side_effect=[1, 2, 3])
    })()

    limiter = RateLimiter(mock_redis, max_requests=2, window_seconds=60)
    is_limited, _, _ = await limiter.is_rate_limited("127.0.0.1")
    assert is_limited is False
    is_limited, _, _ = await limiter.is_rate_limited("127.0.0.1")
    assert is_limited is False
    is_limited, count, retry_after = await limiter.is_rate_limited("127.0.0.1")
    assert is_limited is True
    assert count == 3
    assert retry_after == 30
