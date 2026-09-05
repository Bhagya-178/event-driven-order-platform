from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.outbox import OutboxEvent

class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, event: OutboxEvent) -> None:
        """Adds an outbox event to the current session (does NOT commit)."""
        self.session.add(event)

    async def get_by_aggregate_id(self, aggregate_id: UUID) -> list[OutboxEvent]:
        """Gets all outbox events for a specific aggregate ID in chronological order."""
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == aggregate_id).order_by(OutboxEvent.created_at.asc())
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
