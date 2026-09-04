import pytest
import asyncio
from uuid import uuid4
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.models.inventory import Inventory
from app.db.session import AsyncSessionLocal
from sqlalchemy import select

@pytest.mark.asyncio
async def test_concurrent_inventory_reservations_overselling_prevention():
    """
    Spawns 20 concurrent reservation requests against a product with only 5 units available.
    Verifies that:
    1. Exactly 5 requests succeed (status 201).
    2. Exactly 15 requests fail (status 400 or 409).
    3. Final available_quantity is exactly 0 and reserved_quantity is 5 (never negative).
    """
    product_id = uuid4()

    # Seed product with available_quantity = 5
    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=5, reserved_quantity=0)
        session.add(inv)
        await session.commit()

    async def make_reservation():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "order_id": str(uuid4()),
                "product_id": str(product_id),
                "quantity": 1
            }
            return await client.post("/inventory/reservations", json=payload)

    # Launch 20 concurrent requests
    tasks = [make_reservation() for _ in range(20)]
    responses = await asyncio.gather(*tasks)

    successes = [r for r in responses if r.status_code == 201]
    rejections = [r for r in responses if r.status_code in [400, 409]]

    assert len(successes) == 5, f"Expected exactly 5 successes, got {len(successes)}"
    assert len(rejections) == 15, f"Expected exactly 15 rejections, got {len(rejections)}"

    # Verify database state
    async with AsyncSessionLocal() as session:
        stmt = select(Inventory).where(Inventory.product_id == product_id)
        final_inv = (await session.execute(stmt)).scalars().first()
        assert final_inv.available_quantity == 0
        assert final_inv.reserved_quantity == 5
