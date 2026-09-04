import pytest
from uuid import uuid4
import asyncio
from app.models.inventory import Inventory

@pytest.mark.asyncio
async def test_reserve_inventory_success(async_client, db_session):
    # Seed inventory
    product_id = uuid4()
    inv = Inventory(product_id=product_id, available_quantity=10, reserved_quantity=0)
    db_session.add(inv)
    await db_session.commit()

    payload = {
        "order_id": str(uuid4()),
        "product_id": str(product_id),
        "quantity": 3
    }
    
    response = await async_client.post("/inventory/reservations", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "RESERVED"
    
    # Check inventory state
    await db_session.refresh(inv)
    assert inv.available_quantity == 7
    assert inv.reserved_quantity == 3
    assert inv.version == 2

@pytest.mark.asyncio
async def test_reserve_inventory_insufficient_stock(async_client, db_session):
    product_id = uuid4()
    inv = Inventory(product_id=product_id, available_quantity=1, reserved_quantity=0)
    db_session.add(inv)
    await db_session.commit()

    payload = {
        "order_id": str(uuid4()),
        "product_id": str(product_id),
        "quantity": 2
    }
    
    response = await async_client.post("/inventory/reservations", json=payload)
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_concurrent_reservation_conflict(async_client, db_session):
    product_id = uuid4()
    inv = Inventory(product_id=product_id, available_quantity=5, reserved_quantity=0)
    db_session.add(inv)
    await db_session.commit()

    # Two requests competing for the same 5 units by requesting 4 each (total 8 > 5)
    payload1 = {
        "order_id": str(uuid4()),
        "product_id": str(product_id),
        "quantity": 4
    }
    payload2 = {
        "order_id": str(uuid4()),
        "product_id": str(product_id),
        "quantity": 4
    }

    resp1 = await async_client.post("/inventory/reservations", json=payload1)
    resp2 = await async_client.post("/inventory/reservations", json=payload2)

    # Exactly one must succeed (201) and the second must be rejected (400 or 409)
    status_codes = [resp1.status_code, resp2.status_code]
    assert 201 in status_codes
    assert any(code in [400, 409] for code in status_codes)

    # Verify inventory consistency (never oversold, never negative)
    await db_session.refresh(inv)
    assert inv.available_quantity == 1
    assert inv.reserved_quantity == 4
    assert inv.available_quantity >= 0



