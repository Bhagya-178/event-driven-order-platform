import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.outbox import OutboxEvent
from app.messaging.producer import order_producer
from shared.events.schemas import EventEnvelope

logger = logging.getLogger("app.messaging.outbox_publisher")

class OrderOutboxPublisher:
    """
    Background worker that polls the outbox_events table and publishes
    pending events to Kafka in strict transactional order.
    """
    def __init__(self, batch_size: int = 50, poll_interval: float = 1.0):
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._running = False
        self._task = None

    async def start(self):
        """Starts the background polling loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("OrderOutboxPublisher started.")

    async def stop(self):
        """Stops the background polling loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("OrderOutboxPublisher stopped.")

    async def _poll_loop(self):
        while self._running:
            try:
                published_count = await self.publish_pending_batch()
                if published_count == 0:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in OrderOutboxPublisher loop: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

    _sqlite_lock = asyncio.Lock()

    async def publish_pending_batch(self) -> int:
        """
        Fetches a batch of pending outbox events using FOR UPDATE SKIP LOCKED,
        publishes them to Kafka, and marks them as PUBLISHED atomically.
        """
        async with AsyncSessionLocal() as session:
            is_postgres = session.bind and session.bind.dialect.name == "postgresql"
            if not is_postgres:
                await self._sqlite_lock.acquire()
            try:
                stmt = (
                    select(OutboxEvent)
                    .where(OutboxEvent.status == "PENDING")
                    .order_by(OutboxEvent.created_at.asc())
                    .limit(self.batch_size)
                )
                if is_postgres:
                    stmt = stmt.with_for_update(skip_locked=True)

                result = await session.execute(stmt)
                events = result.scalars().all()

                if not events:
                    return 0

                for event in events:
                    try:
                        envelope = EventEnvelope(
                            event_id=event.id,
                            event_type=event.event_type,
                            occurred_at=event.created_at,
                            aggregate_type=event.aggregate_type,
                            aggregate_id=event.aggregate_id,
                            correlation_id=event.correlation_id or event.id,
                            causation_id=event.causation_id,
                            payload=event.payload
                        )
                        await order_producer.publish_event(envelope)
                        event.status = "PUBLISHED"
                        event.published_at = datetime.now(timezone.utc)
                        event.last_error = None
                        logger.info(f"Relayed outbox event {event.id} ({event.event_type}) to Kafka")
                    except Exception as e:
                        event.retry_count += 1
                        event.last_error = str(e)
                        logger.error(f"Failed to publish outbox event {event.id}: {e}")

                await session.commit()
                return len(events)
            finally:
                if not is_postgres:
                    self._sqlite_lock.release()

order_outbox_publisher = OrderOutboxPublisher()
