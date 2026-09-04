import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock
from sqlalchemy import select, delete
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.models.outbox import OutboxEvent
from app.models.order import Order
from app.models.processed_event import ProcessedEvent
from app.db.session import AsyncSessionLocal
from app.messaging.outbox_publisher import OrderOutboxPublisher
from app.messaging.producer import order_producer
from app.messaging.consumer import order_consumer
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_concurrency_outbox_multi_worker():
    total_events = 20
    event_ids = []

    async with AsyncSessionLocal() as session:
        await session.execute(delete(OutboxEvent))
        await session.commit()

        for i in range(total_events):
            agg_id = uuid4()
            event = OutboxEvent(
                id=uuid4(),
                event_type="OrderCreated",
                aggregate_type="order",
                aggregate_id=agg_id,
                payload={"order_num": i},
                status="PENDING",
                retry_count=0
            )
            session.add(event)
            event_ids.append(event.id)
        await session.commit()

    published_keys = []
    lock = asyncio.Lock()

    async def mock_publish(envelope):
        async with lock:
            published_keys.append(envelope.aggregate_id)

    workers = [
        OrderOutboxPublisher(batch_size=5, poll_interval=0.05),
        OrderOutboxPublisher(batch_size=5, poll_interval=0.05),
    ]

    with patch.object(order_producer, "publish_event", side_effect=mock_publish):
        for _ in range(5):
            tasks = [w.publish_pending_batch() for w in workers]
            await asyncio.gather(*tasks)

    assert len(published_keys) == total_events
    assert len(set(published_keys)) == total_events

@pytest.mark.asyncio
async def test_concurrency_idempotency_api():
    app.dependency_overrides.clear()
    idempotency_key = f"idemp-cross-{uuid4()}"
    customer_id = str(uuid4())
    product_id = str(uuid4())

    async def send_req():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "customer_id": customer_id,
                "items": [{"product_id": product_id, "quantity": 1}]
            }
            return await client.post(
                "/orders/",
                json=payload,
                headers={"Idempotency-Key": idempotency_key}
            )

    tasks = [send_req() for _ in range(8)]
    responses = await asyncio.gather(*tasks)

    for r in responses:
        assert r.status_code == 201
    order_ids = [r.json()["id"] for r in responses]
    assert len(set(order_ids)) == 1
