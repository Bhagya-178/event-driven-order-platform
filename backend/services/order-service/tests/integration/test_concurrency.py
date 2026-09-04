import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock
from sqlalchemy import select

from app.models.outbox import OutboxEvent
from app.db.session import AsyncSessionLocal
from app.messaging.outbox_publisher import OrderOutboxPublisher
from app.messaging.producer import order_producer

@pytest.mark.asyncio
async def test_multi_worker_outbox_concurrency_skip_locked():
    """
    Seeds 30 pending outbox events. Spawns 3 concurrent outbox publisher instances
    polling simultaneously with SELECT ... FOR UPDATE SKIP LOCKED.
    Verifies that:
    1. Exactly 30 events are processed in total across workers.
    2. Every event is published exactly once (no duplicate publishes).
    3. All events reach PUBLISHED status with published_at set.
    """
    total_events = 30
    event_ids = []

    async with AsyncSessionLocal() as session:
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

    # Track which events were published
    published_keys = []
    lock = asyncio.Lock()

    async def mock_publish(envelope):
        async with lock:
            published_keys.append(envelope.aggregate_id)

    # Instantiate 3 separate worker publishers
    workers = [
        OrderOutboxPublisher(batch_size=10, poll_interval=0.1),
        OrderOutboxPublisher(batch_size=10, poll_interval=0.1),
        OrderOutboxPublisher(batch_size=10, poll_interval=0.1),
    ]

    with patch.object(order_producer, "publish_event", side_effect=mock_publish):
        # Run all 3 workers concurrently until all 30 are published
        for _ in range(5):
            tasks = [w.publish_pending_batch() for w in workers]
            await asyncio.gather(*tasks)

    # Verify that all 30 events were published with ZERO duplicates
    assert len(published_keys) == total_events
    assert len(set(published_keys)) == total_events

    # Verify all rows are marked PUBLISHED in DB
    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.id.in_(event_ids))
        results = (await session.execute(stmt)).scalars().all()
        assert len(results) == total_events
        for r in results:
            assert r.status == "PUBLISHED"
            assert r.published_at is not None

from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_concurrent_idempotency_key_requests():
    """
    Fires 10 concurrent HTTP POST requests with the identical Idempotency-Key.
    Verifies that:
    1. All 10 requests succeed (status 201).
    2. All 10 responses return the exact same order id.
    3. Exactly 1 order is persisted in the database.
    """
    app.dependency_overrides.clear()

    idempotency_key = f"idemp-race-{uuid4()}"
    customer_id = str(uuid4())
    product_id = str(uuid4())

    async def send_request():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "customer_id": customer_id,
                "items": [{"product_id": product_id, "quantity": 2}]
            }
            return await client.post(
                "/orders/",
                json=payload,
                headers={"Idempotency-Key": idempotency_key}
            )

    tasks = [send_request() for _ in range(10)]
    responses = await asyncio.gather(*tasks)

    # All requests should return 201 Created
    for r in responses:
        assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"

    # All responses must reference the exact same order id
    order_ids = [r.json()["id"] for r in responses]
    assert len(set(order_ids)) == 1, f"Expected 1 unique order ID, got {len(set(order_ids))}"

    # Verify database has exactly 1 order with this idempotency key
    async with AsyncSessionLocal() as session:
        from app.models.order import Order
        stmt = select(Order).where(Order.idempotency_key == idempotency_key)
        orders = (await session.execute(stmt)).scalars().all()
        assert len(orders) == 1

@pytest.mark.asyncio
async def test_concurrent_cross_topic_order_state_transitions():
    """
    Simultaneously delivers PaymentSucceeded and InventoryReserved events
    from different topics for the same order across concurrent async tasks.
    Verifies that:
    1. Both events are recorded in processed_events.
    2. Order status transitions to CONFIRMED.
    3. payment_status is SUCCEEDED and inventory_status is RESERVED.
    """
    from shared.events.schemas import EventEnvelope
    from app.messaging.consumer import order_consumer
    from app.models.order import Order
    from app.models.processed_event import ProcessedEvent

    order_id = uuid4()
    pay_event_id = uuid4()
    inv_event_id = uuid4()
    corr_id = uuid4()

    # Seed the Order
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

    payment_event = EventEnvelope(
        event_id=pay_event_id,
        event_type="PaymentSucceeded",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={"amount": "100.00", "currency": "USD", "status": "SUCCEEDED"}
    )

    inventory_event = EventEnvelope(
        event_id=inv_event_id,
        event_type="InventoryReserved",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={"items": []}
    )

    # Concurrently process both cross-topic events
    await asyncio.gather(
        order_consumer.process_event(payment_event),
        order_consumer.process_event(inventory_event)
    )

    # Verify final state
    async with AsyncSessionLocal() as session:
        final_order = await session.get(Order, order_id)
        assert final_order is not None
        assert final_order.payment_status == "SUCCEEDED"
        assert final_order.inventory_status == "RESERVED"
        assert final_order.status == "CONFIRMED"

        # Verify processed_events recorded both
        stmt = select(ProcessedEvent).where(ProcessedEvent.event_id.in_([pay_event_id, inv_event_id]))
        processed = (await session.execute(stmt)).scalars().all()
        assert len(processed) == 2
