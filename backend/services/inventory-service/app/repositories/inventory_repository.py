from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.inventory import Inventory, InventoryReservation
from uuid import UUID

class InventoryRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_product_id(self, product_id: UUID) -> Optional[Inventory]:
        result = await self.session.execute(select(Inventory).where(Inventory.product_id == product_id))
        return result.scalars().first()

    def add(self, inventory: Inventory):
        self.session.add(inventory)

class ReservationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, reservation_id: UUID) -> Optional[InventoryReservation]:
        result = await self.session.execute(select(InventoryReservation).where(InventoryReservation.id == reservation_id))
        return result.scalars().first()

    def add(self, reservation: InventoryReservation):
        self.session.add(reservation)
