from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy import select

from app.models.order import Order
from app.models.order_item import OrderItem

class OrderRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, order: Order) -> None:
        """Adds an order to the current session (does NOT commit)."""
        self.session.add(order)

    async def get_by_id(self, order_id: UUID) -> Order | None:
        """Gets an order by ID with its items."""
        stmt = select(Order).options(selectinload(Order.items)).where(Order.id == order_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def list_recent(self, limit: int = 50, offset: int = 0) -> list[Order]:
        """Lists recent orders ordered chronologically descending."""
        stmt = (
            select(Order)
            .options(selectinload(Order.items))
            .order_by(Order.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
