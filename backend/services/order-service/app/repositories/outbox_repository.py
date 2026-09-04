from sqlalchemy.ext.asyncio import AsyncSession
from app.models.outbox import OutboxEvent

class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, event: OutboxEvent) -> None:
        """Adds an outbox event to the current session (does NOT commit)."""
        self.session.add(event)
