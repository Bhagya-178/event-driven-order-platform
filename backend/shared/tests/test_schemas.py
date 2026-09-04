import pytest
from pydantic import ValidationError
from uuid import uuid4
from shared.events.schemas import EventEnvelope

def test_event_envelope_valid():
    agg_id = uuid4()
    corr_id = uuid4()
    
    event = EventEnvelope(
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=agg_id,
        correlation_id=corr_id,
        payload={"foo": "bar"}
    )
    
    assert event.event_type == "OrderCreated"
    assert event.event_version == 1
    assert event.aggregate_id == agg_id
    assert event.correlation_id == corr_id
    assert event.causation_id is None
    assert event.payload == {"foo": "bar"}
    # event_id and occurred_at should be auto-generated
    assert event.event_id is not None
    assert event.occurred_at is not None

def test_event_envelope_invalid():
    with pytest.raises(ValidationError):
        # Missing aggregate_id
        EventEnvelope(
            event_type="OrderCreated",
            aggregate_type="order",
            correlation_id=uuid4(),
            payload={}
        )

def test_causation_id():
    agg_id = uuid4()
    corr_id = uuid4()
    cause_id = uuid4()
    
    event = EventEnvelope(
        event_type="PaymentSucceeded",
        aggregate_type="order",
        aggregate_id=agg_id,
        correlation_id=corr_id,
        causation_id=cause_id,
        payload={}
    )
    assert event.causation_id == cause_id

def test_dead_letter_envelope():
    from shared.events.schemas import DeadLetterEnvelope
    dlq = DeadLetterEnvelope(
        original_topic="orders.events",
        original_partition=1,
        original_offset=42,
        original_key="order-123",
        error_type="ValueError",
        error_message="Invalid payment format",
        retry_count=3,
        payload={"raw": "data"}
    )
    assert dlq.dlq_id is not None
    assert dlq.original_topic == "orders.events"
    assert dlq.original_partition == 1
    assert dlq.original_offset == 42
    assert dlq.retry_count == 3
    assert dlq.failed_at is not None

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_success():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock()
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=2, initial_delay=0.01)
    assert success is True
    assert mock_process.call_count == 1
    assert mock_dlq.call_count == 0

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_exhausted():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock(side_effect=ValueError("Simulated failure"))
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {"test": "val"}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=3, initial_delay=0.01, backoff_factor=1.0)
    assert success is False
    assert mock_process.call_count == 3
    assert mock_dlq.call_count == 1
    dlq_topic, dlq_env = mock_dlq.call_args[0]
    assert dlq_topic == "orders.events.dlq"
    assert dlq_env.error_type == "ValueError"
    assert dlq_env.retry_count == 3
    assert dlq_env.original_offset == 10

def test_dead_letter_envelope():
    from shared.events.schemas import DeadLetterEnvelope
    dlq = DeadLetterEnvelope(
        original_topic="orders.events",
        original_partition=1,
        original_offset=42,
        original_key="order-123",
        error_type="ValueError",
        error_message="Invalid payment format",
        retry_count=3,
        payload={"raw": "data"}
    )
    assert dlq.dlq_id is not None
    assert dlq.original_topic == "orders.events"
    assert dlq.original_partition == 1
    assert dlq.original_offset == 42
    assert dlq.retry_count == 3
    assert dlq.failed_at is not None

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_success():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock()
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=2, initial_delay=0.01)
    assert success is True
    assert mock_process.call_count == 1
    assert mock_dlq.call_count == 0

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_exhausted():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock(side_effect=ValueError("Simulated failure"))
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {"test": "val"}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=3, initial_delay=0.01, backoff_factor=1.0)
    assert success is False
    assert mock_process.call_count == 3
    assert mock_dlq.call_count == 1
    dlq_topic, dlq_env = mock_dlq.call_args[0]
    assert dlq_topic == "orders.events.dlq"
    assert dlq_env.error_type == "ValueError"
    assert dlq_env.retry_count == 3
    assert dlq_env.original_offset == 10

def test_dead_letter_envelope():
    from shared.events.schemas import DeadLetterEnvelope
    dlq = DeadLetterEnvelope(
        original_topic="orders.events",
        original_partition=1,
        original_offset=42,
        original_key="order-123",
        error_type="ValueError",
        error_message="Invalid payment format",
        retry_count=3,
        payload={"raw": "data"}
    )
    assert dlq.dlq_id is not None
    assert dlq.original_topic == "orders.events"
    assert dlq.original_partition == 1
    assert dlq.original_offset == 42
    assert dlq.retry_count == 3
    assert dlq.failed_at is not None

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_success():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock()
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=2, initial_delay=0.01)
    assert success is True
    assert mock_process.call_count == 1
    assert mock_dlq.call_count == 0

@pytest.mark.asyncio
async def test_process_with_retry_and_dlq_exhausted():
    from shared.events.consumer_handler import process_with_retry_and_dlq
    from unittest.mock import AsyncMock

    mock_process = AsyncMock(side_effect=ValueError("Simulated failure"))
    mock_dlq = AsyncMock()
    msg = type("MockMsg", (), {"topic": "orders.events", "partition": 0, "offset": 10, "key": b"key", "value": {"test": "val"}})()

    success = await process_with_retry_and_dlq(msg, mock_process, mock_dlq, "orders.events.dlq", max_retries=3, initial_delay=0.01, backoff_factor=1.0)
    assert success is False
    assert mock_process.call_count == 3
    assert mock_dlq.call_count == 1
    dlq_topic, dlq_env = mock_dlq.call_args[0]
    assert dlq_topic == "orders.events.dlq"
    assert dlq_env.error_type == "ValueError"
    assert dlq_env.retry_count == 3
    assert dlq_env.original_offset == 10

