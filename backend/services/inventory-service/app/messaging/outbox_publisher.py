import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.outbox import OutboxEvent
from app.messaging.producer import inventory_producer
from shared.events.schemas import EventEnvelope

logger = logging.getLogger(__name__)

class InventoryOutboxPublisher:
    def __init__(self, poll_interval: float = 0.5, batch_size: int = 20, max_retries: int = 5):
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.running = False
        self.task = None

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self.run_loop())
        logger.info("InventoryOutboxPublisher background task started")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            logger.info("InventoryOutboxPublisher background task stopped")

    async def run_loop(self):
        while self.running:
            try:
                processed_count = await self.publish_pending_batch()
                if processed_count == 0:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in InventoryOutboxPublisher loop: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

    _sqlite_lock = asyncio.Lock()

    async def publish_pending_batch(self) -> int:
        """
        Fetches pending outbox events using FOR UPDATE SKIP LOCKED,
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
                        await inventory_producer.publish_event(envelope)
                        event.status = "PUBLISHED"
                        event.published_at = datetime.now(timezone.utc)
                        event.last_error = None
                        logger.info(f"Relayed inventory outbox event {event.id} ({event.event_type}) to Kafka")
                    except Exception as e:
                        event.retry_count += 1
                        event.last_error = str(e)
                        logger.warning(
                            f"Failed to publish inventory outbox event {event.id}: {e} (retry {event.retry_count}/{self.max_retries})"
                        )
                        if event.retry_count >= self.max_retries:
                            event.status = "FAILED"
                            logger.error(f"Inventory outbox event {event.id} exceeded max retries. Marked as FAILED.")

                await session.commit()
                return len(events)
            finally:
                if not is_postgres:
                    self._sqlite_lock.release()

inventory_outbox_publisher = InventoryOutboxPublisher()
