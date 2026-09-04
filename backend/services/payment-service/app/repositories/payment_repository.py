from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.payment import Payment
from uuid import UUID

class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, payment_id: UUID) -> Optional[Payment]:
        result = await self.session.execute(select(Payment).where(Payment.id == payment_id))
        return result.scalars().first()

    async def get_by_idempotency_key(self, key: str) -> Optional[Payment]:
        result = await self.session.execute(select(Payment).where(Payment.idempotency_key == key))
        return result.scalars().first()

    def add(self, payment: Payment):
        self.session.add(payment)
