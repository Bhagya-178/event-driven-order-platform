from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError
from app.api.dependencies import get_db
from app.schemas.inventory import ReservationCreate, ReservationResponse, InventoryResponse
from app.services.inventory_service import InventoryService
from app.repositories.inventory_repository import InventoryRepository

router = APIRouter()

@router.post("/reservations", response_model=ReservationResponse, status_code=status.HTTP_201_CREATED)
async def reserve_stock(
    data: ReservationCreate,
    db: AsyncSession = Depends(get_db)
):
    service = InventoryService(db)
    try:
        return await service.reserve_stock(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StaleDataError:
        raise HTTPException(status_code=409, detail="Concurrent modification detected. Please try again.")

@router.post("/reservations/{reservation_id}/release", response_model=ReservationResponse)
async def release_reservation(
    reservation_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    service = InventoryService(db)
    try:
        return await service.release_reservation(reservation_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StaleDataError:
        raise HTTPException(status_code=409, detail="Concurrent modification detected")

@router.post("/reservations/{reservation_id}/confirm", response_model=ReservationResponse)
async def confirm_reservation(
    reservation_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    service = InventoryService(db)
    try:
        return await service.confirm_reservation(reservation_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StaleDataError:
        raise HTTPException(status_code=409, detail="Concurrent modification detected")

@router.get("/{product_id}", response_model=InventoryResponse)
async def get_inventory(
    product_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    repo = InventoryRepository(db)
    inventory = await repo.get_by_product_id(product_id)
    if not inventory:
        raise HTTPException(status_code=404, detail="Product not found")
    return inventory
