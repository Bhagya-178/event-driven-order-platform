import pytest
import asyncio
from uuid import uuid4
from decimal import Decimal
from unittest.mock import patch, AsyncMock

from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.payment import Payment
from app.models.outbox import OutboxEvent
from app.models.processed_event import ProcessedEvent
from app.messaging.consumer import payment_consumer
from app.messaging.outbox_publisher import payment_outbox_publisher
from app.messaging.producer import payment_producer
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_payment_consumer_creates_pending_outbox_event():
    """
    Verifies that processing OrderCreated creates the payment, processed_event,
    and outbox_event in a single atomic DB transaction.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "85.00",
            "currency": "USD",
            "items": []
        }
    )

    await payment_consumer.process_order_created(event)

    async with AsyncSessionLocal() as session:
        # Check payment
        stmt = select(Payment).where(Payment.order_id == order_id)
        payment = (await session.execute(stmt)).scalars().first()
        assert payment is not None
        assert payment.amount == Decimal("85.00")
        assert payment.status == "SUCCEEDED"

        # Check processed_events
        stmt_proc = select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
        processed = (await session.execute(stmt_proc)).scalars().first()
        assert processed is not None

        # Check outbox_events
        stmt_out = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt_out)).scalars().first()
        assert outbox is not None
        assert outbox.event_type == "PaymentSucceeded"
        assert outbox.status == "PENDING"
        assert outbox.correlation_id == corr_id
        assert outbox.causation_id == event_id
        assert outbox.payload["amount"] == "85.00"

@pytest.mark.asyncio
async def test_payment_outbox_publisher_relays_to_kafka():
    """
    Verifies that the PaymentOutboxPublisher relays pending PaymentSucceeded
    events to Kafka and transitions them to PUBLISHED.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "50.00",
            "currency": "USD",
            "items": []
        }
    )

    await payment_consumer.process_order_created(event)

    with patch.object(payment_producer, "publish_event", new_callable=AsyncMock) as mock_publish:
        processed = await payment_outbox_publisher.publish_pending_batch()
        assert processed >= 1
        assert mock_publish.called

        called_envelope = mock_publish.call_args[0][0]
        assert called_envelope.event_type == "PaymentSucceeded"
        assert called_envelope.aggregate_id == order_id
        assert called_envelope.correlation_id == corr_id
        assert called_envelope.causation_id == event_id

    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PUBLISHED"
        assert outbox.published_at is not None

@pytest.mark.asyncio
async def test_payment_outbox_publisher_failure_retry():
    """
    Verifies that if Kafka publishing fails in payment outbox publisher,
    the event remains PENDING and retry_count is incremented.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "25.00",
            "currency": "USD",
            "items": []
        }
    )

    await payment_consumer.process_order_created(event)

    with patch.object(payment_producer, "publish_event", side_effect=RuntimeError("Kafka timeout")):
        processed = await payment_outbox_publisher.publish_pending_batch()
        assert processed >= 1

    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PENDING"
        assert outbox.retry_count == 1
        assert "Kafka timeout" in outbox.last_error
