import pytest
import asyncio
from uuid import uuid4
from decimal import Decimal
from unittest.mock import patch, AsyncMock
from sqlalchemy import select

from app.messaging.consumer import payment_consumer
from app.messaging.producer import payment_producer
from app.models.payment import Payment
from app.models.processed_event import ProcessedEvent
from app.db.session import AsyncSessionLocal
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_poison_pill_unblocks_payment_consumer_partition():
    poison_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 200,
        "key": b"poison",
        "value": {"garbage": "unparseable"}
    })()

    order_id = uuid4()
    valid_event_id = uuid4()
    valid_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 201,
        "key": str(order_id).encode("utf-8"),
        "value": {
            "event_id": str(valid_event_id),
            "event_type": "OrderCreated",
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
            "correlation_id": str(uuid4()),
            "payload": {
                "customer_id": str(uuid4()),
                "total_amount": "50.00",
                "currency": "USD",
                "items": []
            }
        }
    })()

    committed_offsets = []

    with patch.object(payment_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        for msg in [poison_msg, valid_msg]:
            await payment_consumer.handle_message(msg)
            committed_offsets.append(msg.offset)

    assert mock_dlq.call_count == 1
    dlq_topic, dlq_env = mock_dlq.call_args[0]
    assert dlq_topic == "payments.events.dlq"
    assert dlq_env.original_offset == 200

    # Both offsets committed -> partition was unblocked
    assert committed_offsets == [200, 201]

    # Valid event was processed
    async with AsyncSessionLocal() as session:
        stmt = select(Payment).where(Payment.order_id == order_id)
        payment = (await session.execute(stmt)).scalars().first()
        assert payment is not None
        assert payment.amount == Decimal("50.00")

@pytest.mark.asyncio
async def test_payment_at_least_once_redelivery_idempotency():
    order_id = uuid4()
    event_id = uuid4()

    # Pre-seed processed event and payment
    async with AsyncSessionLocal() as session:
        proc = ProcessedEvent(
            event_id=event_id,
            event_type="OrderCreated",
            consumer_name="payment-service"
        )
        session.add(proc)
        pay = Payment(
            order_id=order_id,
            idempotency_key=f"existing-{event_id}",
            amount=Decimal("100.00"),
            currency="USD",
            status="SUCCEEDED"
        )
        session.add(pay)
        await session.commit()

    redelivered_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 75,
        "key": str(order_id).encode("utf-8"),
        "value": {
            "event_id": str(event_id),
            "event_type": "OrderCreated",
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
            "correlation_id": str(uuid4()),
            "payload": {
                "customer_id": str(uuid4()),
                "total_amount": "100.00",
                "currency": "USD",
                "items": []
            }
        }
    })()

    handled = await payment_consumer.handle_message(redelivered_msg)
    assert handled is True

    # Verify no duplicate payment was created
    async with AsyncSessionLocal() as session:
        stmt = select(Payment).where(Payment.order_id == order_id)
        payments = (await session.execute(stmt)).scalars().all()
        assert len(payments) == 1
