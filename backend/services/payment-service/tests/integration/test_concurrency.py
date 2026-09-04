import pytest
import asyncio
from uuid import uuid4
from decimal import Decimal
from unittest.mock import patch, AsyncMock
from sqlalchemy import select, delete
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.models.outbox import OutboxEvent
from app.models.payment import Payment
from app.db.session import AsyncSessionLocal
from app.messaging.outbox_publisher import PaymentOutboxPublisher
from app.messaging.producer import payment_producer

@pytest.mark.asyncio
async def test_multi_worker_payment_outbox_concurrency_skip_locked():
    total_events = 20
    event_ids = []

    async with AsyncSessionLocal() as session:
        await session.execute(delete(OutboxEvent))
        await session.commit()

        for i in range(total_events):
            agg_id = uuid4()
            event = OutboxEvent(
                id=uuid4(),
                event_type="PaymentSucceeded",
                aggregate_type="order",
                aggregate_id=agg_id,
                payload={"amount": "50.00"},
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
        PaymentOutboxPublisher(batch_size=5, poll_interval=0.05),
        PaymentOutboxPublisher(batch_size=5, poll_interval=0.05),
    ]

    with patch.object(payment_producer, "publish_event", side_effect=mock_publish):
        for _ in range(5):
            tasks = [w.publish_pending_batch() for w in workers]
            await asyncio.gather(*tasks)

    assert len(published_keys) == total_events
    assert len(set(published_keys)) == total_events

    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.id.in_(event_ids))
        results = (await session.execute(stmt)).scalars().all()
        assert len(results) == total_events
        for r in results:
            assert r.status == "PUBLISHED"

@pytest.mark.asyncio
async def test_concurrent_payment_api_idempotency_race():
    app.dependency_overrides.clear()
    idempotency_key = f"pay-race-{uuid4()}"
    order_id = str(uuid4())

    async def send_payment():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "order_id": order_id,
                "amount": "100.00",
                "currency": "USD"
            }
            return await client.post(
                "/payments/",
                json=payload,
                headers={"Idempotency-Key": idempotency_key}
            )

    tasks = [send_payment() for _ in range(8)]
    responses = await asyncio.gather(*tasks)

    for r in responses:
        assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"

    payment_ids = [r.json()["id"] for r in responses]
    assert len(set(payment_ids)) == 1

    async with AsyncSessionLocal() as session:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        payments = (await session.execute(stmt)).scalars().all()
        assert len(payments) == 1
