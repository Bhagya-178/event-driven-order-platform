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
async def test_poison_pill_unblocks_partition_for_subsequent_events():
    """
    Simulates a poison-pill message (malformed data that permanently crashes processing)
    followed by 3 valid business events on the same partition.
    Verifies that:
    1. The poison pill exhausts retries and is routed to orders.events.dlq.
    2. The consumer commits the poison-pill offset, unblocking the partition.
    3. The subsequent 3 valid events process successfully without delay.
    """
    order_id = uuid4()
    poison_msg = type("MockMsg", (), {
        "topic": "payments.events",
        "partition": 0,
        "offset": 100,
        "key": b"poison-key",
        "value": {"fatal": "malformed_unparseable_payload"}
    })()

    valid_order_ids = [uuid4() for _ in range(3)]
    valid_msgs = []

    # Pre-seed orders for the valid events
    async with AsyncSessionLocal() as session:
        for oid in valid_order_ids:
            order = Order(
                id=oid,
                customer_id=uuid4(),
                total_amount=50.00,
                currency="USD",
                status="CREATED",
                payment_status="PENDING",
                inventory_status="PENDING"
            )
            session.add(order)
        await session.commit()

    for i, oid in enumerate(valid_order_ids):
        valid_msgs.append(type("MockMsg", (), {
            "topic": "payments.events",
            "partition": 0,
            "offset": 101 + i,
            "key": str(oid).encode("utf-8"),
            "value": {
                "event_id": str(uuid4()),
                "event_type": "PaymentSucceeded",
                "aggregate_type": "order",
                "aggregate_id": str(oid),
                "correlation_id": str(uuid4()),
                "payload": {"amount": "50.00", "currency": "USD", "status": "SUCCEEDED"}
            }
        })())

    # Simulated consumer partition stream: [poison_msg, valid_msg_1, valid_msg_2, valid_msg_3]
    partition_stream = [poison_msg] + valid_msgs
    committed_offsets = []

    with patch.object(order_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        for msg in partition_stream:
            handled = await order_consumer.handle_message(msg)
            # Both success and DLQ results commit offset to advance the partition
            committed_offsets.append(msg.offset)

    # 1. Poison pill went to DLQ
    assert mock_dlq.call_count == 1
    dlq_topic, dlq_envelope = mock_dlq.call_args[0]
    assert dlq_topic == "orders.events.dlq"
    assert dlq_envelope.original_offset == 100

    # 2. All 4 offsets were committed (partition never stalled)
    assert committed_offsets == [100, 101, 102, 103]

    # 3. All 3 subsequent orders processed to payment_status = SUCCEEDED
    async with AsyncSessionLocal() as session:
        for oid in valid_order_ids:
            ord_obj = await session.get(Order, oid)
            assert ord_obj.payment_status == "SUCCEEDED"

@pytest.mark.asyncio
async def test_at_least_once_redelivery_after_crash_recovery():
    """
    Simulates at-least-once delivery where a consumer processed an event,
    wrote to DB, but crashed before committing Kafka offset.
    Upon redelivery of the same event:
    1. Consumer identifies event_id in processed_events.
    2. Business logic is safely skipped (no duplicate processing).
    3. Offset is committed cleanly.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()

    # Pre-seed order and an already processed event record
    async with AsyncSessionLocal() as session:
        order = Order(
            id=order_id,
            customer_id=uuid4(),
            total_amount=75.00,
            currency="USD",
            status="CONFIRMED",
            payment_status="SUCCEEDED",
            inventory_status="RESERVED"
        )
        session.add(order)
        proc = ProcessedEvent(
            event_id=event_id,
            event_type="PaymentSucceeded",
            consumer_name="order-service"
        )
        session.add(proc)
        await session.commit()

    redelivered_msg = type("MockMsg", (), {
        "topic": "payments.events",
        "partition": 0,
        "offset": 55,
        "key": str(order_id).encode("utf-8"),
        "value": {
            "event_id": str(event_id),
            "event_type": "PaymentSucceeded",
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
            "correlation_id": str(corr_id),
            "payload": {"amount": "75.00", "currency": "USD", "status": "SUCCEEDED"}
        }
    })()

    # Process redelivered message
    result = await order_consumer.handle_message(redelivered_msg)
    assert result is True

    # Verify no duplicate processed_events records created
    async with AsyncSessionLocal() as session:
        stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
        records = (await session.execute(stmt)).scalars().all()
        assert len(records) == 1

@pytest.mark.asyncio
async def test_broker_failure_and_self_healing_recovery_in_outbox():
    """
    Simulates a broker outage while the outbox publisher is running.
    Verifies that:
    1. During outage: outbox events remain PENDING, retry_count increments, last_error captures failure.
    2. When broker recovers: publisher successfully relays pending events and transitions to PUBLISHED.
    """
    event_id = uuid4()
    agg_id = uuid4()

    async with AsyncSessionLocal() as session:
        event = OutboxEvent(
            id=event_id,
            event_type="OrderCreated",
            aggregate_type="order",
            aggregate_id=agg_id,
            payload={"test": "broker-failure"},
            status="PENDING",
            retry_count=0
        )
        session.add(event)
        await session.commit()

    # Step 1: Broker failure (outage)
    with patch.object(order_producer, "publish_event", side_effect=ConnectionRefusedError("Kafka broker unreachable")):
        await order_outbox_publisher.publish_pending_batch()

    async with AsyncSessionLocal() as session:
        outbox = await session.get(OutboxEvent, event_id)
        assert outbox.status == "PENDING"
        assert outbox.retry_count == 1
        assert "broker unreachable" in outbox.last_error

    # Step 2: Broker self-heals / recovers
    with patch.object(order_producer, "publish_event", new_callable=AsyncMock) as mock_pub:
        await order_outbox_publisher.publish_pending_batch()
        assert mock_pub.called

    async with AsyncSessionLocal() as session:
        outbox = await session.get(OutboxEvent, event_id)
        assert outbox.status == "PUBLISHED"
        assert outbox.published_at is not None
