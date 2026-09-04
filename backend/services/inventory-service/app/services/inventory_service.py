import asyncio
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError
from app.repositories.inventory_repository import InventoryRepository, ReservationRepository
from app.models.inventory import Inventory, InventoryReservation
from app.schemas.inventory import ReservationCreate

class InventoryService:
    _sqlite_lock = asyncio.Lock()

    def __init__(self, session: AsyncSession):
        self.session = session
        self.inv_repo = InventoryRepository(session)
        self.res_repo = ReservationRepository(session)

    async def reserve_stock(self, data: ReservationCreate, max_retries: int = 10) -> InventoryReservation:
        is_postgres = self.session.bind and self.session.bind.dialect.name == "postgresql"
        if not is_postgres:
            await self._sqlite_lock.acquire()
        try:
            for attempt in range(max_retries):
                try:
                    inventory = await self.inv_repo.get_by_product_id(data.product_id)
                    if not inventory:
                        raise ValueError("Product not found")

                    if inventory.available_quantity < data.quantity:
                        raise ValueError("Insufficient stock")

                    # Optimistic locking relies on version. Modifying available_quantity increments it on flush automatically.
                    inventory.available_quantity -= data.quantity
                    inventory.reserved_quantity += data.quantity
                    
                    reservation = InventoryReservation(
                        order_id=data.order_id,
                        product_id=data.product_id,
                        quantity=data.quantity,
                        status="RESERVED"
                    )
                    self.res_repo.add(reservation)

                    await self.session.commit()
                    return reservation
                except StaleDataError:
                    await self.session.rollback()
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(0.01 * (attempt + 1))
        finally:
            if not is_postgres:
                self._sqlite_lock.release()

    async def release_reservation(self, reservation_id: UUID) -> InventoryReservation:
        reservation = await self.res_repo.get_by_id(reservation_id)
        if not reservation:
            raise ValueError("Reservation not found")
        
        if reservation.status != "RESERVED":
            raise ValueError(f"Cannot release reservation in status {reservation.status}")
        
        inventory = await self.inv_repo.get_by_product_id(reservation.product_id)
        
        reservation.status = "RELEASED"
        inventory.reserved_quantity -= reservation.quantity
        inventory.available_quantity += reservation.quantity
        
        await self.session.commit()
        return reservation

    async def confirm_reservation(self, reservation_id: UUID) -> InventoryReservation:
        reservation = await self.res_repo.get_by_id(reservation_id)
        if not reservation:
            raise ValueError("Reservation not found")
        
        if reservation.status != "RESERVED":
            raise ValueError(f"Cannot confirm reservation in status {reservation.status}")
        
        inventory = await self.inv_repo.get_by_product_id(reservation.product_id)
        
        reservation.status = "CONFIRMED"
        inventory.reserved_quantity -= reservation.quantity
        # Available quantity remains unchanged, as it was decremented during RESERVE
        
        await self.session.commit()
        return reservation
