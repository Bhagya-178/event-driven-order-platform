import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock
from sqlalchemy import select

from app.messaging.consumer import order_consumer
from app.messaging.producer import order_producer
from app.messaging.outbox_publisher import order_outbox_publisher
from app.models.outbox import OutboxEvent
from app.models.processed_event import ProcessedEvent
from app.models.order import Order
from app.db.session import AsyncSessionLocal
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_failure_poison_pill_unblocking():
    poison_msg = type("MockMsg", (), {
        "topic": "payments.events",
        "partition": 0,
        "offset": 500,
        "key": b"poison",
        "value": {"bad": "data"}
    })()

    with patch.object(order_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        handled = await order_consumer.handle_message(poison_msg)
        assert handled is False
        assert mock_dlq.call_count == 1
        dlq_topic, dlq_env = mock_dlq.call_args[0]
        assert dlq_topic == "orders.events.dlq"
        assert dlq_env.retry_count == 3
        assert dlq_env.original_offset == 500

@pytest.mark.asyncio
async def test_failure_outbox_broker_outage_recovery():
    event_id = uuid4()
    async with AsyncSessionLocal() as session:
        event = OutboxEvent(
            id=event_id,
            event_type="OrderCreated",
            aggregate_type="order",
            aggregate_id=uuid4(),
            payload={"fail": "broker"},
            status="PENDING",
            retry_count=0
        )
        session.add(event)
        await session.commit()

    with patch.object(order_producer, "publish_event", side_effect=ConnectionError("Broker offline")):
        await order_outbox_publisher.publish_pending_batch()

    async with AsyncSessionLocal() as session:
        ev = await session.get(OutboxEvent, event_id)
        assert ev.status == "PENDING"
        assert ev.retry_count == 1
        assert "Broker offline" in ev.last_error

    # Recovery
    with patch.object(order_producer, "publish_event", new_callable=AsyncMock):
        await order_outbox_publisher.publish_pending_batch()

    async with AsyncSessionLocal() as session:
        ev = await session.get(OutboxEvent, event_id)
        assert ev.status == "PUBLISHED"
