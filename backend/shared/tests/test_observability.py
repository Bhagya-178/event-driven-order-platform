import pytest
import json
import logging
from unittest.mock import AsyncMock, patch
from shared.observability.logging import JSONFormatter
from shared.observability.metrics import (
    http_requests_total,
    metrics_response,
    dlq_events_total
)
from shared.redis.client import RedisManager
from shared.redis.rate_limiter import RateLimiter

def test_json_formatter():
    formatter = JSONFormatter(service_name="test-service")
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None
    )
    record.correlation_id = "1234-uuid"
    formatted = formatter.format(record)
    data = json.loads(formatted)

    assert data["service"] == "test-service"
    assert data["level"] == "INFO"
    assert data["message"] == "Test message"
    assert data["correlation_id"] == "1234-uuid"
    assert "timestamp" in data

@pytest.mark.asyncio
async def test_metrics_endpoint():
    dlq_events_total.labels(topic="orders.events.dlq", error_type="ValueError").inc()
    resp = await metrics_response()
    assert resp.status_code == 200
    assert "dlq_events_routed_total" in resp.body.decode("utf-8")

@pytest.mark.asyncio
async def test_redis_manager_fail_open_when_disconnected():
    manager = RedisManager(redis_url="redis://localhost:9999/0")
    # Client is None -> should fail open gracefully
    assert await manager.ping() is False
    assert await manager.get("any_key") is None
    assert await manager.set("key", "val") is False
    assert await manager.increment("counter") == 1

@pytest.mark.asyncio
async def test_rate_limiter_limit_enforced():
    mock_redis = RedisManager()
    limiter = RateLimiter(mock_redis, max_requests=2, window_seconds=60)

    # Mock redis increment returning counts 1, 2, 3
    with patch.object(mock_redis, "increment", side_effect=[1, 2, 3]):
        mock_redis.client = type("MockClient", (), {"ttl": AsyncMock(return_value=45)})()

        # Request 1: allowed
        is_limited, count, _ = await limiter.is_rate_limited("client-1")
        assert is_limited is False
        assert count == 1

        # Request 2: allowed
        is_limited, count, _ = await limiter.is_rate_limited("client-1")
        assert is_limited is False
        assert count == 2

        # Request 3: limited!
        is_limited, count, retry_after = await limiter.is_rate_limited("client-1")
        assert is_limited is True
        assert count == 3
        assert retry_after == 45
